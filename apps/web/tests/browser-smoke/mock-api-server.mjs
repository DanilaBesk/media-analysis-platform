// FILE: apps/web/tests/browser-smoke/mock-api-server.mjs
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Provide an app-local browser smoke harness that simulates the Web UI API and event boundaries without moving business logic into the client.
// SCOPE: Serve packet-local REST and WebSocket endpoints for create, list, details, child-action, artifact-resolve, and reconciliation verification during browser runs.
// DEPENDS: M-WEB-UI, M-API-HTTP, M-API-EVENTS
// LINKS: V-M-WEB-UI
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added a standalone mock API and websocket transport for packet-local browser verification.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   serve-rest-surface - Expose packet-local REST endpoints required by the Web UI browser smoke flow.
//   serve-event-stream - Expose a minimal WebSocket stream that triggers authoritative REST reconciliation.
//   keep-browser-proof-local - Keep verification scaffolding inside apps/web tests without changing API ownership.
// END_MODULE_MAP

import { createHash } from "node:crypto";
import { createServer } from "node:http";

const HOST = process.env.HOST ?? "127.0.0.1";
const PORT = Number(process.env.PORT ?? "8080");
const WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

let sequence = 0;

const jobs = new Map();
const jobEvents = new Map();
const sockets = new Set();

function nowIso() {
  return new Date().toISOString();
}

function nextId(prefix) {
  sequence += 1;
  return `${prefix}-${sequence}`;
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Content-Type": "application/json",
  });
  response.end(JSON.stringify(payload));
}

function sendNoContent(response) {
  response.writeHead(204, {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  });
  response.end();
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    request.on("data", (chunk) => {
      chunks.push(Buffer.from(chunk));
    });
    request.on("end", () => {
      resolve(Buffer.concat(chunks));
    });
    request.on("error", reject);
  });
}

function parseJson(buffer) {
  if (buffer.length === 0) {
    return {};
  }
  return JSON.parse(buffer.toString("utf8"));
}

function parseMultipartFilenames(buffer) {
  const names = [];
  const pattern = /filename="([^"]+)"/g;
  const raw = buffer.toString("utf8");
  let match = pattern.exec(raw);
  while (match) {
    names.push(match[1]);
    match = pattern.exec(raw);
  }
  return names;
}

function listRootJobs() {
  return Array.from(jobs.values())
    .filter((job) => job.job_id === job.root_job_id)
    .sort((left, right) => right.created_at.localeCompare(left.created_at));
}

function getEvents(jobId) {
  return jobEvents.get(jobId) ?? [];
}

function setJob(job) {
  jobs.set(job.job_id, job);
}

function pushEvent(jobId, event) {
  const events = getEvents(jobId);
  events.push(event);
  jobEvents.set(jobId, events);
}

function makeSourceItem(position, name, kind = "uploaded_file", url = null) {
  return {
    position,
    source: {
      source_id: nextId("source"),
      source_kind: kind,
      original_filename: name,
      display_name: name,
      source_url: url,
    },
  };
}

function makeArtifact(jobId, artifactKind, filename) {
  return {
    artifact_id: `${jobId}-${artifactKind}`,
    artifact_kind: artifactKind,
    filename,
    mime_type: "text/plain",
    size_bytes: 256,
    created_at: nowIso(),
  };
}

function makeEvent(job, eventType, payload = {}) {
  return {
    event_id: nextId("event"),
    event_type: eventType,
    job_id: job.job_id,
    root_job_id: job.root_job_id,
    version: job.version,
    emitted_at: nowIso(),
    payload: {
      status: job.status,
      ...payload,
    },
  };
}

function toStreamEnvelope(job, eventType, payload = {}) {
  return {
    ...makeEvent(job, eventType, payload),
    job_type: job.job_type,
    job_url: `/v1/jobs/${job.job_id}`,
  };
}

function registerRootJob(job) {
  setJob(job);
  pushEvent(job.job_id, makeEvent(job, "job.created"));
  return job;
}

function createTranscriptionJob({ displayName, filenames, sourceKind, sourceUrl = null, inputKind }) {
  const jobId = nextId("job");
  const createdAt = nowIso();
  const sourceItems = filenames.map((name, position) => makeSourceItem(position, name, sourceKind, sourceUrl));
  const job = {
    job_id: jobId,
    root_job_id: jobId,
    job_type: "transcription",
    status: "queued",
    version: 1,
    display_name: displayName,
    delivery: { strategy: "polling" },
    source_set: {
      source_set_id: nextId("source-set"),
      input_kind: inputKind,
      items: sourceItems,
    },
    artifacts: [],
    children: [],
    created_at: createdAt,
    started_at: null,
    finished_at: null,
    cancel_requested_at: null,
    progress: {
      stage: "accepted",
      message: "Queued in browser smoke harness",
    },
  };
  return registerRootJob(job);
}

function emitWsJson(socket, payload) {
  const body = Buffer.from(JSON.stringify(payload), "utf8");
  if (body.length >= 126) {
    const header = Buffer.from([0x81, 126, body.length >> 8, body.length & 255]);
    socket.write(Buffer.concat([header, body]));
    return;
  }
  socket.write(Buffer.concat([Buffer.from([0x81, body.length]), body]));
}

function broadcast(payload) {
  for (const socket of sockets) {
    emitWsJson(socket, payload);
  }
}

function updateRootJob(jobId, patch, eventType, eventPayload = {}) {
  const current = jobs.get(jobId);
  if (!current) {
    return null;
  }
  const next = {
    ...current,
    ...patch,
  };
  setJob(next);
  pushEvent(jobId, makeEvent(next, eventType, eventPayload));
  return next;
}

function completeJobWithVersionGap(jobId) {
  const current = jobs.get(jobId);
  if (!current) {
    return;
  }
  const artifact = makeArtifact(jobId, "transcript_plain", `${jobId}.txt`);
  const next = {
    ...current,
    status: "succeeded",
    version: 3,
    artifacts: [artifact],
    started_at: current.started_at ?? current.created_at,
    finished_at: nowIso(),
    progress: {
      stage: "completed",
      message: "Artifact created in browser smoke harness",
    },
  };
  setJob(next);
  pushEvent(jobId, makeEvent(next, "job.completed", { progress_stage: "completed" }));
  broadcast(
    toStreamEnvelope(next, "job.completed", {
      progress_stage: "completed",
      progress_message: "browser smoke reconciliation trigger",
    }),
  );
}

function startJob(jobId) {
  const current = jobs.get(jobId);
  if (!current) {
    return;
  }
  const next = {
    ...current,
    status: "running",
    version: 1,
    started_at: current.created_at,
    progress: {
      stage: "processing",
      message: "Authoritative job is running",
    },
  };
  setJob(next);
  pushEvent(jobId, makeEvent(next, "job.updated", { progress_stage: "processing" }));
}

function attachChildJob(rootJobId, childJob) {
  const root = jobs.get(rootJobId);
  if (!root) {
    return;
  }
  const childReference = {
    job_id: childJob.job_id,
    job_type: childJob.job_type,
    status: childJob.status,
    version: childJob.version,
    job_url: `/v1/jobs/${childJob.job_id}`,
    root_job_id: childJob.root_job_id,
  };
  const nextRoot = {
    ...root,
    version: root.version + 1,
    children: [...root.children, childReference],
  };
  setJob(nextRoot);
  pushEvent(rootJobId, makeEvent(nextRoot, "job.updated"));
}

function createChildJob(parentJob, jobType) {
  const jobId = nextId("job");
  const createdAt = nowIso();
  const child = {
    job_id: jobId,
    root_job_id: parentJob.root_job_id,
    parent_job_id: parentJob.job_id,
    job_type: jobType,
    status: "queued",
    version: 1,
    display_name: `${jobType} job`,
    delivery: { strategy: "polling" },
    source_set: parentJob.source_set,
    artifacts: [],
    children: [],
    created_at: createdAt,
    started_at: null,
    finished_at: null,
    cancel_requested_at: null,
    progress: {
      stage: "accepted",
      message: `${jobType} queued`,
    },
  };
  setJob(child);
  attachChildJob(parentJob.root_job_id, child);
  return child;
}

async function handleRequest(request, response) {
  const url = new URL(request.url ?? "/", `http://${HOST}:${PORT}`);
  if (request.method === "OPTIONS") {
    sendNoContent(response);
    return;
  }

  if (request.method === "GET" && url.pathname === "/v1/jobs") {
    sendJson(response, 200, {
      items: listRootJobs(),
      page: Number(url.searchParams.get("page") ?? "1"),
      page_size: Number(url.searchParams.get("page_size") ?? "20"),
      next_page: null,
    });
    return;
  }

  if (request.method === "POST" && url.pathname === "/v1/transcription-jobs") {
    const body = await readBody(request);
    const filenames = parseMultipartFilenames(body);
    const createdJobs = filenames.map((filename) =>
      createTranscriptionJob({
        displayName: filename,
        filenames: [filename],
        sourceKind: "uploaded_file",
        inputKind: "single_source",
      }),
    );
    createdJobs.forEach((job) => {
      startJob(job.job_id);
      setTimeout(() => {
        completeJobWithVersionGap(job.job_id);
      }, 500);
    });
    sendJson(response, 200, { jobs: createdJobs });
    return;
  }

  if (request.method === "POST" && url.pathname === "/v1/transcription-jobs/combined") {
    const body = await readBody(request);
    const filenames = parseMultipartFilenames(body);
    const job = createTranscriptionJob({
      displayName: `Combined: ${filenames.join(", ")}`,
      filenames,
      sourceKind: "uploaded_file",
      inputKind: "combined_upload",
    });
    startJob(job.job_id);
    setTimeout(() => {
      completeJobWithVersionGap(job.job_id);
    }, 500);
    sendJson(response, 200, { job });
    return;
  }

  if (request.method === "POST" && url.pathname === "/v1/transcription-jobs/from-url") {
    const body = parseJson(await readBody(request));
    const sourceUrl = String(body.url ?? "https://youtu.be/demo");
    const displayName = String(body.display_name ?? "URL transcription");
    const job = createTranscriptionJob({
      displayName,
      filenames: [sourceUrl],
      sourceKind: "youtube_url",
      sourceUrl,
      inputKind: "single_source",
    });
    startJob(job.job_id);
    setTimeout(() => {
      completeJobWithVersionGap(job.job_id);
    }, 800);
    sendJson(response, 200, { job });
    return;
  }

  const jobMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)$/);
  if (request.method === "GET" && jobMatch) {
    const job = jobs.get(jobMatch[1]);
    if (!job) {
      sendJson(response, 404, {
        error: {
          code: "job_not_found",
          message: `Job ${jobMatch[1]} was not found`,
        },
      });
      return;
    }
    sendJson(response, 200, { job });
    return;
  }

  const eventsMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)\/events$/);
  if (request.method === "GET" && eventsMatch) {
    const jobId = eventsMatch[1];
    const rootJob = jobs.get(jobId);
    const rootId = rootJob?.root_job_id ?? jobId;
    sendJson(response, 200, { items: getEvents(rootId) });
    return;
  }

  const reportMatch = url.pathname.match(/^\/v1\/transcription-jobs\/([^/]+)\/report-jobs$/);
  if (request.method === "POST" && reportMatch) {
    const parentJob = jobs.get(reportMatch[1]);
    if (!parentJob) {
      sendJson(response, 404, { error: { code: "job_not_found", message: "Parent job missing" } });
      return;
    }
    const child = createChildJob(parentJob, "report");
    sendJson(response, 200, { job: child });
    return;
  }

  const researchMatch = url.pathname.match(/^\/v1\/report-jobs\/([^/]+)\/deep-research-jobs$/);
  if (request.method === "POST" && researchMatch) {
    const parentJob = jobs.get(researchMatch[1]);
    if (!parentJob) {
      sendJson(response, 404, { error: { code: "job_not_found", message: "Parent job missing" } });
      return;
    }
    const child = createChildJob(parentJob, "deep_research");
    sendJson(response, 200, { job: child });
    return;
  }

  const cancelMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)\/cancel$/);
  if (request.method === "POST" && cancelMatch) {
    const job = updateRootJob(
      cancelMatch[1],
      {
        status: "cancel_requested",
        version: (jobs.get(cancelMatch[1])?.version ?? 1) + 1,
        cancel_requested_at: nowIso(),
      },
      "job.updated",
      { progress_stage: "cancel_requested" },
    );
    if (!job) {
      sendJson(response, 404, { error: { code: "job_not_found", message: "Job missing" } });
      return;
    }
    sendJson(response, 200, { job });
    return;
  }

  const retryMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)\/retry$/);
  if (request.method === "POST" && retryMatch) {
    const current = jobs.get(retryMatch[1]);
    if (!current) {
      sendJson(response, 404, { error: { code: "job_not_found", message: "Job missing" } });
      return;
    }
    const retried = {
      ...current,
      status: "running",
      version: current.version + 1,
      finished_at: null,
      progress: {
        stage: "retrying",
        message: "Retry requested in smoke harness",
      },
    };
    setJob(retried);
    pushEvent(retried.root_job_id, makeEvent(retried, "job.updated", { progress_stage: "retrying" }));
    sendJson(response, 200, { job: retried });
    return;
  }

  const artifactMatch = url.pathname.match(/^\/v1\/artifacts\/([^/]+)$/);
  if (request.method === "GET" && artifactMatch) {
    const artifactId = artifactMatch[1];
    const job = Array.from(jobs.values()).find((candidate) =>
      candidate.artifacts.some((artifact) => artifact.artifact_id === artifactId),
    );
    if (!job) {
      sendJson(response, 404, {
        error: {
          code: "artifact_not_found",
          message: `Artifact ${artifactId} was not found`,
        },
      });
      return;
    }
    const artifact = job.artifacts.find((candidate) => candidate.artifact_id === artifactId);
    sendJson(response, 200, {
      artifact_id: artifact.artifact_id,
      job_id: job.job_id,
      artifact_kind: artifact.artifact_kind,
      filename: artifact.filename,
      mime_type: artifact.mime_type,
      size_bytes: artifact.size_bytes,
      created_at: artifact.created_at,
      download: {
        provider: "minio_presigned_url",
        url: `https://downloads.example.test/${artifact.artifact_id}`,
        expires_at: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
      },
    });
    return;
  }

  sendJson(response, 404, {
    error: {
      code: "route_not_found",
      message: `No mock route for ${request.method} ${url.pathname}`,
    },
  });
}

// START_BLOCK_BLOCK_START_BROWSER_SMOKE_SERVER
const server = createServer((request, response) => {
  void handleRequest(request, response).catch((error) => {
    sendJson(response, 500, {
      error: {
        code: "mock_server_failure",
        message: error instanceof Error ? error.message : "Unknown mock server failure",
      },
    });
  });
});

server.on("upgrade", (request, socket) => {
  const url = new URL(request.url ?? "/", `http://${HOST}:${PORT}`);
  if (url.pathname !== "/v1/ws") {
    socket.destroy();
    return;
  }
  const key = request.headers["sec-websocket-key"];
  if (typeof key !== "string") {
    socket.destroy();
    return;
  }
  const accept = createHash("sha1")
    .update(`${key}${WS_MAGIC}`)
    .digest("base64");
  socket.write(
    [
      "HTTP/1.1 101 Switching Protocols",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Accept: ${accept}`,
      "",
      "",
    ].join("\r\n"),
  );
  sockets.add(socket);
  socket.on("close", () => {
    sockets.delete(socket);
  });
  socket.on("end", () => {
    sockets.delete(socket);
  });
  socket.on("error", () => {
    sockets.delete(socket);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`browser-smoke-mock listening on http://${HOST}:${PORT}`);
});
// END_BLOCK_BLOCK_START_BROWSER_SMOKE_SERVER
