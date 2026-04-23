// FILE: apps/web/src/features/jobs/jobs-shell.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Implement the Web UI jobs and job-details routes with API-backed create, monitor, lineage, and reconciliation behavior.
// SCOPE: Render jobs list filters and create forms, drive authoritative polling reads, reconcile websocket updates back to REST, and expose child or admin actions without absorbing API business logic.
// DEPENDS: M-WEB-UI, M-API-HTTP, M-API-EVENTS, M-CONTRACTS
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Replaced packet placeholders with jobs list, create flows, job details, lineage actions, and polling-truth reconciliation.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   render-jobs-workspace - Provide create-job forms, list filters, polling-backed monitoring, and navigation to job details.
//   render-job-details-workspace - Provide authoritative job details, action buttons, event timeline, and artifact-slot context for the route.
//   reconcile-event-stream - Keep websocket as a low-latency refresh trigger while polling remains the truth path.
// END_MODULE_MAP

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { Link, useParams } from "react-router-dom";

import { useWebUiRuntime } from "../../app/runtime-context";
import {
  RECONCILE_STATE_MARKER,
  WebUiApiClientError,
  requiresRestReconciliation,
} from "../../lib/api/client";
import type {
  ArtifactResolution,
  ChildActionDraft,
  DeliveryDraft,
  JobEventStreamEnvelope,
  JobEventView,
  JobListResponse,
  JobSnapshot,
  JobStatus,
  JobType,
  ListJobsFilter,
  UploadDraft,
  UrlDraft,
} from "../../lib/api/types";

const POLL_INTERVAL_MS = 3_000;
const RECONNECT_DELAY_MS = 1_000;

const ACTIVE_JOB_STATUSES = new Set<JobStatus>(["queued", "running", "cancel_requested"]);
const TERMINAL_JOB_STATUSES = new Set<JobStatus>(["succeeded", "failed", "canceled"]);

interface JobDetailsRouteModel {
  snapshot: JobSnapshot | null;
  events: JobEventView[];
  artifactResolutions: Record<string, ArtifactResolution>;
  loading: boolean;
  loadingLabel: string;
  error: string | null;
  actionError: string | null;
  artifactError: string | null;
  actionPending: string | null;
  resolvingArtifactId: string | null;
  childActionDraft: ChildActionDraft;
  setChildActionDraft: (draft: ChildActionDraft) => void;
  refresh: (reason: string) => Promise<void>;
  runAction: (kind: "report" | "deep_research" | "cancel" | "retry") => Promise<void>;
  resolveArtifact: (artifactId: string) => Promise<void>;
}

const JobDetailsContext = createContext<JobDetailsRouteModel | null>(null);

function formatDate(value?: string | null): string {
  if (!value) {
    return "not set";
  }
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatBytes(value: number): string {
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function toErrorMessage(error: unknown): string {
  if (error instanceof WebUiApiClientError) {
    return `${error.message} (${error.status})`;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return "Unknown UI error";
}

function isActiveJob(job: Pick<JobSnapshot, "status">): boolean {
  return ACTIVE_JOB_STATUSES.has(job.status);
}

function isRelevantEvent(
  event: JobEventStreamEnvelope,
  routeJobId: string,
  snapshot: JobSnapshot | null,
): boolean {
  if (event.job_id === routeJobId || event.root_job_id === routeJobId) {
    return true;
  }
  if (!snapshot) {
    return false;
  }
  return event.root_job_id === snapshot.root_job_id;
}

function defaultDeliveryDraft(): DeliveryDraft {
  return {
    strategy: "polling",
    webhookUrl: "",
  };
}

function defaultChildActionDraft(): ChildActionDraft {
  return {
    clientRef: "",
    delivery: defaultDeliveryDraft(),
  };
}

function defaultUploadDraft(): UploadDraft {
  return {
    files: [],
    displayName: "",
    clientRef: "",
    delivery: defaultDeliveryDraft(),
  };
}

function defaultUrlDraft(): UrlDraft {
  return {
    url: "",
    displayName: "",
    clientRef: "",
    delivery: defaultDeliveryDraft(),
  };
}

function useJobsWorkspaceModel() {
  const { apiClient } = useWebUiRuntime();
  const [filters, setFilters] = useState<ListJobsFilter>({ page: 1, page_size: 20 });
  const [response, setResponse] = useState<JobListResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [submitKind, setSubmitKind] = useState<"per_file" | "combined" | "url">("per_file");
  const [uploadDraft, setUploadDraft] = useState<UploadDraft>(defaultUploadDraft);
  const [urlDraft, setUrlDraft] = useState<UrlDraft>(defaultUrlDraft);
  const [submitPending, setSubmitPending] = useState(false);
  const [submitMessage, setSubmitMessage] = useState<string | null>(null);
  const [fileInputKey, setFileInputKey] = useState(0);
  const loadRequestIdRef = useRef(0);

  const loadJobs = useCallback(async () => {
    const requestId = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestId;
    setLoading(true);
    setError(null);
    try {
      const next = await apiClient.listJobs(filters);
      if (requestId !== loadRequestIdRef.current) {
        return;
      }
      setResponse(next);
    } catch (nextError) {
      if (requestId !== loadRequestIdRef.current) {
        return;
      }
      setError(toErrorMessage(nextError));
    } finally {
      if (requestId === loadRequestIdRef.current) {
        setLoading(false);
      }
    }
  }, [apiClient, filters]);

  useEffect(() => {
    void loadJobs();
  }, [loadJobs]);

  const shouldPoll = Boolean(response?.items.some(isActiveJob));

  useEffect(() => {
    if (!shouldPoll) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      void loadJobs();
    }, POLL_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [loadJobs, shouldPoll]);

  const handleSubmit = useCallback(async () => {
    setSubmitPending(true);
    setSubmitMessage(null);
    setError(null);
    try {
      if (submitKind === "url") {
        const job = await apiClient.createFromUrl(urlDraft);
        setSubmitMessage(`Created URL job ${job.job_id}`);
        await loadJobs();
        return;
      }

      if (uploadDraft.files.length === 0) {
        throw new Error("Select at least one file before creating jobs.");
      }

      if (submitKind === "combined") {
        const job = await apiClient.createCombinedUpload(uploadDraft);
        setSubmitMessage(`Created combined job ${job.job_id}`);
        setUploadDraft(defaultUploadDraft());
        setFileInputKey((value) => value + 1);
        await loadJobs();
        return;
      }

      const jobs = await apiClient.createUpload(uploadDraft);
      setSubmitMessage(`Created ${jobs.length} upload job${jobs.length === 1 ? "" : "s"}.`);
      setUploadDraft(defaultUploadDraft());
      setFileInputKey((value) => value + 1);
      await loadJobs();
    } catch (nextError) {
      setError(toErrorMessage(nextError));
    } finally {
      setSubmitPending(false);
    }
  }, [apiClient, loadJobs, submitKind, uploadDraft, urlDraft]);

  return {
    filters,
    setFilters,
    response,
    loading,
    error,
    loadJobs,
    submitKind,
    setSubmitKind,
    uploadDraft,
    setUploadDraft,
    urlDraft,
    setUrlDraft,
    submitPending,
    submitMessage,
    handleSubmit,
    fileInputKey,
  };
}

function useJobDetailsRouteController(jobId: string): JobDetailsRouteModel {
  const { apiClient } = useWebUiRuntime();
  const [snapshot, setSnapshot] = useState<JobSnapshot | null>(null);
  const [events, setEvents] = useState<JobEventView[]>([]);
  const [artifactResolutions, setArtifactResolutions] = useState<Record<string, ArtifactResolution>>({});
  const [loading, setLoading] = useState(true);
  const [loadingLabel, setLoadingLabel] = useState("Loading authoritative job snapshot...");
  const [error, setError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [artifactError, setArtifactError] = useState<string | null>(null);
  const [actionPending, setActionPending] = useState<string | null>(null);
  const [resolvingArtifactId, setResolvingArtifactId] = useState<string | null>(null);
  const [childActionDraft, setChildActionDraft] = useState<ChildActionDraft>(defaultChildActionDraft);
  const snapshotRef = useRef<JobSnapshot | null>(null);
  const refreshRequestIdRef = useRef(0);

  const refresh = useCallback(
    async (reason: string) => {
      const requestId = refreshRequestIdRef.current + 1;
      refreshRequestIdRef.current = requestId;
      setLoadingLabel(
        reason === "initial"
          ? "Loading authoritative job snapshot..."
          : `Refreshing authoritative state (${reason})...`,
      );
      if (reason === "initial") {
        setLoading(true);
      }
      setError(null);
      try {
        const [nextSnapshot, nextEvents] = await Promise.all([
          apiClient.getJob(jobId),
          apiClient.listJobEvents(jobId),
        ]);
        if (requestId !== refreshRequestIdRef.current) {
          return;
        }
        snapshotRef.current = nextSnapshot;
        setSnapshot(nextSnapshot);
        setEvents(nextEvents);
      } catch (nextError) {
        if (requestId !== refreshRequestIdRef.current) {
          return;
        }
        setError(toErrorMessage(nextError));
      } finally {
        if (requestId === refreshRequestIdRef.current) {
          setLoading(false);
        }
      }
    },
    [apiClient, jobId],
  );

  useEffect(() => {
    void refresh("initial");
  }, [refresh]);

  const shouldPoll = Boolean(
    snapshot &&
      (isActiveJob(snapshot) || snapshot.children.some((child) => ACTIVE_JOB_STATUSES.has(child.status))),
  );

  useEffect(() => {
    if (!shouldPoll) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      void refresh("poll");
    }, POLL_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [refresh, shouldPoll]);

  useEffect(() => {
    let closed = false;
    let didOpen = false;
    let reconnectTimer: number | undefined;
    let subscription = null as ReturnType<typeof apiClient.subscribeToJobEvents> | null;

    const subscribe = (isReconnect: boolean) => {
      subscription = apiClient.subscribeToJobEvents({
        onOpen: () => {
          if (didOpen || isReconnect) {
            console.info("%s reason=reconnect route_job_id=%s", RECONCILE_STATE_MARKER, jobId);
            void refresh("reconnect");
          }
          didOpen = true;
        },
        onMessage: (event) => {
          if (!isRelevantEvent(event, jobId, snapshotRef.current)) {
            return;
          }
          const lastSeenVersion = snapshotRef.current?.version ?? 0;
          const needsGapReconcile = requiresRestReconciliation(lastSeenVersion, event.version);
          const reason = needsGapReconcile ? "version_gap" : "ws_event";
          console.info(
            "%s reason=%s route_job_id=%s event_job_id=%s last_seen_version=%d incoming_version=%d",
            RECONCILE_STATE_MARKER,
            reason,
            jobId,
            event.job_id,
            lastSeenVersion,
            event.version,
          );
          void refresh(reason);
        },
        onClose: () => {
          if (closed) {
            return;
          }
          if (reconnectTimer) {
            window.clearTimeout(reconnectTimer);
          }
          reconnectTimer = window.setTimeout(() => {
            if (closed) {
              return;
            }
            subscribe(true);
          }, RECONNECT_DELAY_MS);
        },
      });
    };

    subscribe(false);

    return () => {
      closed = true;
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
      }
      subscription?.close();
    };
  }, [apiClient, jobId, refresh]);

  const runAction = useCallback(
    async (kind: "report" | "deep_research" | "cancel" | "retry") => {
      setActionPending(kind);
      setActionError(null);
      try {
        switch (kind) {
          case "report":
            await apiClient.createReport(jobId, childActionDraft);
            await refresh("report");
            break;
          case "deep_research":
            await apiClient.createDeepResearch(jobId, childActionDraft);
            await refresh("deep_research");
            break;
          case "cancel":
            await apiClient.cancelJob(jobId);
            await refresh("cancel");
            break;
          case "retry":
            await apiClient.retryJob(jobId);
            await refresh("retry");
            break;
        }
      } catch (nextError) {
        setActionError(toErrorMessage(nextError));
      } finally {
        setActionPending(null);
      }
    },
    [apiClient, childActionDraft, jobId, refresh],
  );

  const resolveArtifact = useCallback(
    async (artifactId: string) => {
      setResolvingArtifactId(artifactId);
      setArtifactError(null);
      try {
        const resolution = await apiClient.resolveArtifact(artifactId);
        setArtifactResolutions((current) => ({
          ...current,
          [artifactId]: resolution,
        }));
      } catch (nextError) {
        setArtifactError(toErrorMessage(nextError));
      } finally {
        setResolvingArtifactId(null);
      }
    },
    [apiClient],
  );

  return {
    snapshot,
    events,
    artifactResolutions,
    loading,
    loadingLabel,
    error,
    actionError,
    artifactError,
    actionPending,
    resolvingArtifactId,
    childActionDraft,
    setChildActionDraft,
    refresh,
    runAction,
    resolveArtifact,
  };
}

export function useJobDetailsRouteModel(): JobDetailsRouteModel {
  const model = useContext(JobDetailsContext);
  if (!model) {
    throw new Error("JobDetailsRouteShell must provide JobDetailsContext before artifact views render.");
  }
  return model;
}

// START_BLOCK_BLOCK_RENDER_JOBS_ROUTE_SHELL
export function JobsRouteShell(): JSX.Element {
  const {
    error,
    fileInputKey,
    filters,
    handleSubmit,
    loading,
    response,
    setFilters,
    setSubmitKind,
    setUploadDraft,
    setUrlDraft,
    submitKind,
    submitMessage,
    submitPending,
    uploadDraft,
    urlDraft,
  } = useJobsWorkspaceModel();

  return (
    <div className="shell-grid">
      <section className="shell-stack">
        <section className="shell-card" aria-label="Create jobs">
          <div className="page-toolbar">
            <div>
              <h2>Create jobs</h2>
              <p className="muted-text">
                The UI stays inside API-owned semantics: it submits create requests, then monitors the
                authoritative snapshots returned by polling reads.
              </p>
            </div>
            <label>
              Mode
              <select
                aria-label="Submission mode"
                value={submitKind}
                onChange={(event) =>
                  setSubmitKind(event.target.value as "per_file" | "combined" | "url")
                }
              >
                <option value="per_file">Per-file upload</option>
                <option value="combined">Combined upload</option>
                <option value="url">YouTube URL</option>
              </select>
            </label>
          </div>

          <div className="shell-form">
            {submitKind === "url" ? (
              <>
                <div className="shell-form__row">
                  <label>
                    YouTube URL
                    <input
                      aria-label="YouTube URL"
                      placeholder="https://youtu.be/..."
                      value={urlDraft.url}
                      onChange={(event) =>
                        setUrlDraft((current) => ({ ...current, url: event.target.value }))
                      }
                    />
                  </label>
                </div>
                <div className="shell-form__inline">
                  <label>
                    Display name
                    <input
                      aria-label="URL display name"
                      value={urlDraft.displayName}
                      onChange={(event) =>
                        setUrlDraft((current) => ({ ...current, displayName: event.target.value }))
                      }
                    />
                  </label>
                  <label>
                    Client ref
                    <input
                      aria-label="URL client ref"
                      value={urlDraft.clientRef}
                      onChange={(event) =>
                        setUrlDraft((current) => ({ ...current, clientRef: event.target.value }))
                      }
                    />
                  </label>
                </div>
                <DeliveryFields
                  draft={urlDraft.delivery}
                  onChange={(delivery) =>
                    setUrlDraft((current) => ({
                      ...current,
                      delivery,
                    }))
                  }
                />
              </>
            ) : (
              <>
                <div className="shell-form__row">
                  <label>
                    Files
                    <input
                      key={fileInputKey}
                      aria-label="Upload files"
                      multiple
                      type="file"
                      onChange={(event) =>
                        setUploadDraft((current) => ({
                          ...current,
                          files: Array.from(event.target.files ?? []),
                        }))
                      }
                    />
                  </label>
                  <p className="helper-text">
                    {submitKind === "combined"
                      ? "Combined upload creates one ordered transcription job for all selected files."
                      : "Per-file upload creates one transcription job per selected file."}
                  </p>
                </div>
                <div className="shell-form__inline">
                  <label>
                    Display name
                    <input
                      aria-label="Upload display name"
                      value={uploadDraft.displayName}
                      disabled={submitKind === "per_file" && uploadDraft.files.length > 1}
                      onChange={(event) =>
                        setUploadDraft((current) => ({
                          ...current,
                          displayName: event.target.value,
                        }))
                      }
                    />
                  </label>
                  <label>
                    Client ref
                    <input
                      aria-label="Upload client ref"
                      value={uploadDraft.clientRef}
                      onChange={(event) =>
                        setUploadDraft((current) => ({
                          ...current,
                          clientRef: event.target.value,
                        }))
                      }
                    />
                  </label>
                </div>
                <DeliveryFields
                  draft={uploadDraft.delivery}
                  onChange={(delivery) =>
                    setUploadDraft((current) => ({
                      ...current,
                      delivery,
                    }))
                  }
                />
              </>
            )}

            <button disabled={submitPending} onClick={() => void handleSubmit()}>
              {submitPending ? "Submitting..." : "Create job"}
            </button>
            {submitMessage ? <p className="success-text">{submitMessage}</p> : null}
            {error ? <p className="error-text">{error}</p> : null}
          </div>
        </section>

        <section className="shell-card" aria-label="Jobs list">
          <div className="page-toolbar">
            <div>
              <h2>Jobs</h2>
              <p className="muted-text">
                Polling remains authoritative. The list refreshes automatically while active jobs remain.
              </p>
            </div>
            <div className="pagination">
              <button
                className="pagination-button"
                disabled={(filters.page ?? 1) <= 1}
                onClick={() =>
                  setFilters((current) => ({
                    ...current,
                    page: Math.max(1, (current.page ?? 1) - 1),
                  }))
                }
              >
                Previous
              </button>
              <span>Page {filters.page ?? 1}</span>
              <button
                className="pagination-button"
                disabled={!response?.next_page}
                onClick={() =>
                  setFilters((current) => ({
                    ...current,
                    page: response?.next_page ?? (current.page ?? 1),
                  }))
                }
              >
                Next
              </button>
            </div>
          </div>

          <div className="shell-form__inline">
            <label>
              Status
              <select
                aria-label="Filter by status"
                value={filters.status ?? ""}
                onChange={(event) =>
                  setFilters((current) => ({
                    ...current,
                    page: 1,
                    status: event.target.value as ListJobsFilter["status"],
                  }))
                }
              >
                <option value="">Any</option>
                <option value="queued">Queued</option>
                <option value="running">Running</option>
                <option value="cancel_requested">Cancel requested</option>
                <option value="succeeded">Succeeded</option>
                <option value="failed">Failed</option>
                <option value="canceled">Canceled</option>
              </select>
            </label>
            <label>
              Job type
              <select
                aria-label="Filter by job type"
                value={filters.job_type ?? ""}
                onChange={(event) =>
                  setFilters((current) => ({
                    ...current,
                    page: 1,
                    job_type: event.target.value as ListJobsFilter["job_type"],
                  }))
                }
              >
                <option value="">Any</option>
                <option value="transcription">Transcription</option>
                <option value="report">Report</option>
                <option value="deep_research">Deep research</option>
              </select>
            </label>
            <label>
              Root job id
              <input
                aria-label="Filter by root job id"
                value={filters.root_job_id ?? ""}
                onChange={(event) =>
                  setFilters((current) => ({
                    ...current,
                    page: 1,
                    root_job_id: event.target.value,
                  }))
                }
              />
            </label>
          </div>

          {loading ? <p className="muted-text">Refreshing jobs...</p> : null}
          {!loading && response?.items.length === 0 ? (
            <p className="muted-text">No jobs matched the current filters.</p>
          ) : null}

          <div className="job-list">
            {response?.items.map((job) => (
              <article className="job-card" key={job.job_id}>
                <div className="job-card__header">
                  <div>
                    <Link className="job-link" to={`/jobs/${job.job_id}`}>
                      {job.display_name || `${job.job_type} job`}
                    </Link>
                    <p className="muted-text">
                      <code className="inline-code">{job.job_id}</code>
                    </p>
                  </div>
                  <span className="status-pill" data-status={job.status}>
                    {job.status}
                  </span>
                </div>
                <dl className="job-card__meta">
                  <div>
                    <dt>Type</dt>
                    <dd>{job.job_type}</dd>
                  </div>
                  <div>
                    <dt>Version</dt>
                    <dd>{job.version}</dd>
                  </div>
                  <div>
                    <dt>Artifacts</dt>
                    <dd>{job.artifacts.length}</dd>
                  </div>
                  <div>
                    <dt>Children</dt>
                    <dd>{job.children.length}</dd>
                  </div>
                  <div>
                    <dt>Created</dt>
                    <dd>{formatDate(job.created_at)}</dd>
                  </div>
                </dl>
              </article>
            ))}
          </div>
        </section>
      </section>

      <section className="shell-stack">
        <section className="shell-card" aria-label="Monitoring notes">
          <h2>Monitoring contract</h2>
          <ul className="shell-list">
            <li>Polling remains the authoritative read path for list and details views.</li>
            <li>WebSocket only accelerates refresh by triggering a REST re-read.</li>
            <li>Cancel, retry, report, deep research, and artifact resolve stay API-owned.</li>
          </ul>
        </section>
      </section>
    </div>
  );
}
// END_BLOCK_BLOCK_RENDER_JOBS_ROUTE_SHELL

interface JobDetailsRouteShellProps {
  artifactsSlot: ReactNode;
}

// START_BLOCK_BLOCK_RENDER_JOB_DETAILS_ROUTE_SHELL
export function JobDetailsRouteShell({
  artifactsSlot,
}: JobDetailsRouteShellProps): JSX.Element {
  const { jobId = "pending-job-id" } = useParams();
  const model = useJobDetailsRouteController(jobId);

  const actionButtons = useMemo(() => {
    const snapshot = model.snapshot;
    if (!snapshot) {
      return [];
    }

    const buttons: Array<{ key: "report" | "deep_research" | "cancel" | "retry"; label: string }> = [];
    if (snapshot.job_type === "transcription" && snapshot.status === "succeeded") {
      buttons.push({ key: "report", label: "Create report job" });
    }
    if (snapshot.job_type === "report" && snapshot.status === "succeeded") {
      buttons.push({ key: "deep_research", label: "Create deep research job" });
    }
    if (snapshot.status === "queued" || snapshot.status === "running") {
      buttons.push({ key: "cancel", label: "Cancel job" });
    }
    if (TERMINAL_JOB_STATUSES.has(snapshot.status)) {
      buttons.push({ key: "retry", label: "Retry job" });
    }
    return buttons;
  }, [model.snapshot]);

  return (
    <JobDetailsContext.Provider value={model}>
      <div className="shell-stack">
        <section className="shell-card" aria-label="Job details workspace">
          <div className="page-toolbar">
            <div>
              <h2>Job details</h2>
              <p className="muted-text">
                Route job id: <code className="inline-code">{jobId}</code>
              </p>
            </div>
            <button
              className="pagination-button"
              disabled={model.loading}
              onClick={() => void model.refresh("manual")}
            >
              Refresh now
            </button>
          </div>

          {model.loading ? <p className="muted-text">{model.loadingLabel}</p> : null}
          {model.error ? <p className="error-text">{model.error}</p> : null}

          {model.snapshot ? (
            <>
              <div className="details-grid">
                <div>
                  <dt>Status</dt>
                  <dd>
                    <span className="status-pill" data-status={model.snapshot.status}>
                      {model.snapshot.status}
                    </span>
                  </dd>
                </div>
                <div>
                  <dt>Type</dt>
                  <dd>{model.snapshot.job_type}</dd>
                </div>
                <div>
                  <dt>Version</dt>
                  <dd>{model.snapshot.version}</dd>
                </div>
                <div>
                  <dt>Delivery</dt>
                  <dd>{model.snapshot.delivery.strategy}</dd>
                </div>
                <div>
                  <dt>Created</dt>
                  <dd>{formatDate(model.snapshot.created_at)}</dd>
                </div>
                <div>
                  <dt>Started</dt>
                  <dd>{formatDate(model.snapshot.started_at)}</dd>
                </div>
                <div>
                  <dt>Finished</dt>
                  <dd>{formatDate(model.snapshot.finished_at)}</dd>
                </div>
                <div>
                  <dt>Cancel requested</dt>
                  <dd>{formatDate(model.snapshot.cancel_requested_at)}</dd>
                </div>
              </div>

              {model.snapshot.progress ? (
                <p className="helper-text">
                  Progress: {model.snapshot.progress.stage}
                  {model.snapshot.progress.message ? ` - ${model.snapshot.progress.message}` : ""}
                </p>
              ) : null}
              {model.snapshot.latest_error ? (
                <p className="error-text">
                  Latest error: {model.snapshot.latest_error.code}
                  {model.snapshot.latest_error.message
                    ? ` - ${model.snapshot.latest_error.message}`
                    : ""}
                </p>
              ) : null}

              <section className="shell-stack">
                <div className="shell-form__inline">
                  <label>
                    Child client ref
                    <input
                      aria-label="Child client ref"
                      value={model.childActionDraft.clientRef}
                      onChange={(event) =>
                        model.setChildActionDraft({
                          ...model.childActionDraft,
                          clientRef: event.target.value,
                        })
                      }
                    />
                  </label>
                </div>
                <DeliveryFields
                  draft={model.childActionDraft.delivery}
                  onChange={(delivery) =>
                    model.setChildActionDraft({
                      ...model.childActionDraft,
                      delivery,
                    })
                  }
                />
                <div className="action-bar">
                  {actionButtons.map((button) => (
                    <button
                      key={button.key}
                      className={`action-button ${button.key === "cancel" ? "action-button--secondary" : ""}`}
                      disabled={model.actionPending !== null}
                      onClick={() => void model.runAction(button.key)}
                    >
                      {model.actionPending === button.key ? "Working..." : button.label}
                    </button>
                  ))}
                </div>
                {model.actionError ? <p className="error-text">{model.actionError}</p> : null}
              </section>

              <section className="shell-card">
                <h3>Sources</h3>
                <div className="source-list">
                  {model.snapshot.source_set.items.map((item) => (
                    <div className="timeline-entry" key={item.source.source_id}>
                      <strong>
                        #{item.position + 1} {item.source.display_name || item.source.original_filename || item.source.source_kind}
                      </strong>
                      <span className="muted-text">
                        {item.source.source_url || item.source.original_filename || item.source.source_kind}
                      </span>
                    </div>
                  ))}
                </div>
              </section>

              <section className="shell-card">
                <h3>Child jobs</h3>
                {model.snapshot.children.length === 0 ? (
                  <p className="muted-text">No child jobs yet.</p>
                ) : (
                  <div className="job-list">
                    {model.snapshot.children.map((child) => (
                      <article className="job-card" key={child.job_id}>
                        <Link className="job-link" to={`/jobs/${child.job_id}`}>
                          {child.job_type}
                        </Link>
                        <span className="status-pill" data-status={child.status}>
                          {child.status}
                        </span>
                        <p className="muted-text">
                          <code className="inline-code">{child.job_id}</code>
                        </p>
                      </article>
                    ))}
                  </div>
                )}
              </section>

              {artifactsSlot}

              <section className="shell-card">
                <h3>Event timeline</h3>
                {model.events.length === 0 ? (
                  <p className="muted-text">No events recorded yet.</p>
                ) : (
                  <div className="timeline-list">
                    {model.events.map((event) => (
                      <article className="timeline-entry" key={event.event_id}>
                        <strong>{event.event_type}</strong>
                        <span className="status-pill" data-status={event.payload.status}>
                          {event.payload.status}
                        </span>
                        <span className="muted-text">{formatDate(event.emitted_at)}</span>
                        <span className="muted-text">
                          version {event.version}
                          {event.payload.progress_stage ? ` - ${event.payload.progress_stage}` : ""}
                          {event.payload.progress_message ? ` - ${event.payload.progress_message}` : ""}
                        </span>
                      </article>
                    ))}
                  </div>
                )}
              </section>
            </>
          ) : null}
        </section>
      </div>
    </JobDetailsContext.Provider>
  );
}
// END_BLOCK_BLOCK_RENDER_JOB_DETAILS_ROUTE_SHELL

// START_BLOCK_BLOCK_RENDER_NOT_FOUND_SHELL
export function RouteNotFoundShell(): JSX.Element {
  return (
    <section className="shell-card" aria-label="Route not found shell">
      <h2>Route not available in this packet</h2>
      <p>
        This packet implements the jobs workspace and job details routes only. Other views still need a
        future packet.
      </p>
    </section>
  );
}
// END_BLOCK_BLOCK_RENDER_NOT_FOUND_SHELL

function DeliveryFields({
  draft,
  onChange,
}: {
  draft: DeliveryDraft;
  onChange: (draft: DeliveryDraft) => void;
}): JSX.Element {
  return (
    <div className="shell-form__inline">
      <label>
        Delivery strategy
        <select
          aria-label="Delivery strategy"
          value={draft.strategy}
          onChange={(event) =>
            onChange({
              ...draft,
              strategy: event.target.value as DeliveryDraft["strategy"],
            })
          }
        >
          <option value="polling">Polling</option>
          <option value="webhook">Webhook</option>
        </select>
      </label>
      <label>
        Webhook URL
        <input
          aria-label="Webhook URL"
          placeholder="https://example.com/hook"
          value={draft.webhookUrl}
          disabled={draft.strategy !== "webhook"}
          onChange={(event) =>
            onChange({
              ...draft,
              webhookUrl: event.target.value,
            })
          }
        />
      </label>
    </div>
  );
}
