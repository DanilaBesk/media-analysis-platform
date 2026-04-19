# Telegram Transcriber Platform Monorepo Migration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Migrate the current single-process `telegram-transcriber-bot` into a local-first monorepo platform with a Go API control plane, Python execution workers, React web UI, thin Telegram bot, separate MCP server, Redis queues, PostgreSQL state, MinIO artifact storage, Docker Compose orchestration, and first-class polling, WebSocket, retry, and cancellation contracts.

**Architecture:** The target system separates orchestration from execution. The Go API server owns job creation, state transitions, artifact metadata, job graph relationships, polling/WS status delivery, and cancel/retry semantics. Python workers own heavy execution for transcription, report generation, and deep research by reusing the existing Python pipeline. Telegram bot, web UI, and MCP become thin adapters over the same HTTP API.

**Tech Stack:** Go API server, PostgreSQL, Redis, MinIO, React + TypeScript web app, Python workers, Python Telegram bot, TypeScript MCP server, Docker Compose, CPU-only faster-whisper in Docker.

---

## 1. Purpose of This Document

This document is the single source of truth for the migration from the current monolithic Telegram bot to a multi-application local platform.

This plan must be used for:

- architecture alignment;
- implementation sequencing;
- API and event contract definition;
- monorepo bootstrapping;
- worker split design;
- UI, Telegram, and MCP client adaptation;
- Docker Compose rollout;
- verification and cutover.

This document intentionally includes both design and execution detail. It is not a lightweight RFC. It is meant to be sufficient for implementation without needing to rediscover major architectural decisions.

## 2. Current State Summary

The current repository is a single Python application with one runtime entrypoint:

- [src/telegram_transcriber_bot/__main__.py](/Users/danila/work/my/telegram-transcriber-bot/src/telegram_transcriber_bot/__main__.py:1)

The current logic is distributed as follows:

- [src/telegram_transcriber_bot/bot.py](/Users/danila/work/my/telegram-transcriber-bot/src/telegram_transcriber_bot/bot.py:1)
  Telegram runtime, source extraction orchestration, attachment download, media group buffering, callback handlers, status messages, and result delivery.
- [src/telegram_transcriber_bot/service.py](/Users/danila/work/my/telegram-transcriber-bot/src/telegram_transcriber_bot/service.py:1)
  Current application service for processing sources, writing job metadata, generating report artifacts, and running deep research.
- [src/telegram_transcriber_bot/transcribers.py](/Users/danila/work/my/telegram-transcriber-bot/src/telegram_transcriber_bot/transcribers.py:1)
  YouTube transcript fast path and Whisper fallback.
- [src/telegram_transcriber_bot/documents.py](/Users/danila/work/my/telegram-transcriber-bot/src/telegram_transcriber_bot/documents.py:1)
  Markdown and DOCX artifact rendering.
- [src/telegram_transcriber_bot/cglm_runner.py](/Users/danila/work/my/telegram-transcriber-bot/src/telegram_transcriber_bot/cglm_runner.py:1)
  Report generation subprocess adapter.
- [src/telegram_transcriber_bot/deep_research.py](/Users/danila/work/my/telegram-transcriber-bot/src/telegram_transcriber_bot/deep_research.py:1)
  Deep research multi-phase pipeline.

Current storage model:

- local files under `.data/jobs/<job_id>/...`
- local metadata JSON per job
- no external queue
- no DB
- no external object storage
- no HTTP API
- no Web UI
- no MCP server

Current user-visible capabilities to preserve:

- YouTube transcription
- Telegram voice/audio/video/document transcription
- media group handling
- plain transcript output
- segmented transcript output
- report generation
- deep research generation
- artifact reuse inside the current process

## 3. Non-Goals for the First Platform Version

The following are explicitly out of scope for the first migration:

- public internet deployment;
- production-grade security model;
- user authentication and authorization;
- quotas, billing, or rate limiting;
- multi-tenant user isolation;
- GPU support for Whisper;
- global dedupe/reuse across uploads and URLs;
- distributed multi-region deployment;
- Kubernetes;
- high-availability failover architecture;
- S3-compatible cloud object storage outside local MinIO.

## 4. Architectural Principles

The migration must preserve these principles:

1. All business logic lives behind the API server boundary.
2. Telegram bot, web UI, and MCP are transport adapters only.
3. Workers perform heavy execution and never expose public transport contracts.
4. PostgreSQL is the persistent system of record for jobs and artifact metadata.
5. Redis is ephemeral infrastructure for queues and event fanout, not the source of truth.
6. MinIO stores source files and output artifacts; clients do not write to it directly.
7. Polling is the correctness fallback. WebSocket is an optimization layer.
8. Retry creates a new job. It does not mutate historical job execution.
9. Cancellation is cooperative and explicit.
10. Migration should reuse the working Python execution core where that is cheaper and safer than rewriting.

## 5. Recommended Target Architecture

### 5.1 High-Level Components

- `apps/api`
  Go HTTP API and WebSocket server.
- `workers/transcription`
  Python worker for transcription jobs.
- `workers/report`
  Python worker for report generation jobs.
- `workers/deep-research`
  Python worker for deep research jobs.
- `apps/web`
  React UI for job creation, monitoring, and downloads.
- `apps/telegram-bot`
  Thin Python bot adapter over HTTP API.
- `apps/mcp-server`
  Thin MCP adapter over HTTP API.
- `packages/contracts`
  OpenAPI definitions, JSON Schemas, WS envelope schemas, and shared enums.
- `packages/sdk-ts`
  Typed client for web UI and MCP server.
- `packages/sdk-py`
  Typed or thin Python client for Telegram bot and optional internal tools.
- `infra/docker-compose.yml`
  Local deployment topology.

### 5.2 Control Plane vs Execution Plane

**Control plane responsibilities**

- accept uploads and source creation requests;
- create jobs and child jobs;
- persist job state in PostgreSQL;
- store source files in MinIO;
- enqueue work in Redis;
- stream job updates via WebSocket;
- expose polling endpoints;
- expose artifact download endpoints;
- implement retry and cancel semantics.

**Execution plane responsibilities**

- execute transcription;
- execute report generation;
- execute deep research generation;
- upload artifacts to MinIO;
- emit progress updates back to API;
- respond to cancellation checks;
- record structured failures.

## 6. Target Monorepo Layout

Recommended repository shape:

```text
.
├── apps
│   ├── api
│   │   ├── cmd/api
│   │   ├── internal
│   │   │   ├── api
│   │   │   ├── jobs
│   │   │   ├── storage
│   │   │   ├── queue
│   │   │   ├── ws
│   │   │   └── db
│   │   └── migrations
│   ├── web
│   │   ├── src
│   │   │   ├── app
│   │   │   ├── pages
│   │   │   ├── components
│   │   │   ├── features/jobs
│   │   │   ├── features/artifacts
│   │   │   └── lib/api
│   ├── telegram-bot
│   │   ├── src/telegram_transcriber_bot_client
│   │   └── tests
│   └── mcp-server
│       ├── src
│       └── package.json
├── workers
│   ├── common
│   │   └── src/transcriber_workers_common
│   ├── transcription
│   │   ├── src
│   │   └── tests
│   ├── report
│   │   ├── src
│   │   └── tests
│   └── deep-research
│       ├── src
│       └── tests
├── packages
│   ├── contracts
│   │   ├── openapi
│   │   ├── schemas
│   │   └── ws
│   ├── sdk-ts
│   └── sdk-py
├── infra
│   ├── docker-compose.yml
│   ├── env
│   ├── minio
│   └── init
├── docs
│   ├── architecture
│   ├── api
│   └── plans
└── legacy
    └── telegram-transcriber-bot
```

Notes:

- The current single-app Python code should not be deleted immediately.
- Existing code should be migrated in waves and only then either removed or archived under `legacy/`.
- If moving the current repository into monorepo root is too disruptive for an early wave, the first wave may create `apps/api`, `apps/web`, `workers/*`, and `packages/*` alongside the current `src/telegram_transcriber_bot`, then clean up later.

## 7. Runtime Topology

### 7.1 Local Docker Compose Topology

Required services:

- `postgres`
- `redis`
- `minio`
- `minio-init`
- `api`
- `worker-transcription`
- `worker-report`
- `worker-deep-research`
- `web`
- `telegram-bot`
- `mcp-server`

### 7.2 Network Assumptions

- local deployment only;
- no auth layer in v1;
- services communicate on a private Docker network;
- only `web`, optionally `api`, and optionally `minio` admin console are published to localhost;
- `telegram-bot` and `mcp-server` call API by internal service DNS name.

### 7.3 CPU-Only Whisper

First version must assume CPU-only transcription:

- no CUDA dependency;
- no GPU runtime assumptions in Docker Compose;
- model cache persisted via Docker volume;
- `WHISPER_MODEL=turbo` allowed but CPU-only performance must be considered in worker concurrency.

## 8. Domain Model

### 8.1 Core Entities

The platform should use the following conceptual entities:

- `Source`
- `Job`
- `Artifact`
- `JobEvent`
- `JobRelation`

### 8.2 Job Types

The system should have explicit job types:

- `transcription`
- `report`
- `deep_research`

### 8.3 Job Statuses

Recommended status enum:

- `queued`
- `running`
- `succeeded`
- `failed`
- `cancel_requested`
- `canceled`

### 8.4 Artifact Kinds

Recommended artifact kinds:

- `source_original`
- `transcript_plain`
- `transcript_segmented_markdown`
- `transcript_docx`
- `report_markdown`
- `report_docx`
- `deep_research_markdown`
- `execution_log`

## 9. PostgreSQL Schema

The first version does not need a large normalized schema. It needs a reliable operational schema.

### 9.1 Table: `sources`

Suggested columns:

- `id uuid primary key`
- `source_kind text not null`
  Values:
  - `uploaded_file`
  - `youtube_url`
  - `telegram_upload`
- `display_name text not null`
- `original_filename text null`
- `mime_type text null`
- `source_url text null`
- `object_key text not null`
- `size_bytes bigint null`
- `created_at timestamptz not null default now()`

### 9.2 Table: `jobs`

Suggested columns:

- `id uuid primary key`
- `root_job_id uuid not null`
- `parent_job_id uuid null`
- `retry_of_job_id uuid null`
- `job_type text not null`
- `status text not null`
- `source_id uuid null`
- `version bigint not null default 1`
- `progress_stage text not null default 'queued'`
- `progress_message text not null default ''`
- `error_code text null`
- `error_message text null`
- `cancel_requested_at timestamptz null`
- `created_at timestamptz not null default now()`
- `started_at timestamptz null`
- `finished_at timestamptz null`

Indexes:

- `(root_job_id)`
- `(parent_job_id)`
- `(retry_of_job_id)`
- `(status, created_at desc)`
- `(job_type, created_at desc)`

### 9.3 Table: `job_artifacts`

Suggested columns:

- `id uuid primary key`
- `job_id uuid not null`
- `artifact_kind text not null`
- `format text not null`
- `filename text not null`
- `mime_type text not null`
- `object_key text not null`
- `size_bytes bigint null`
- `created_at timestamptz not null default now()`

Unique constraints:

- `(job_id, artifact_kind, format)`

### 9.4 Table: `job_events`

Suggested columns:

- `id uuid primary key`
- `job_id uuid not null`
- `event_type text not null`
- `version bigint not null`
- `payload jsonb not null`
- `created_at timestamptz not null default now()`

Indexes:

- `(job_id, created_at asc)`
- `(root_job_id?)`

If cross-job stream becomes important, add `root_job_id` here from the start.

### 9.5 Optional Table: `job_links`

If parent/root columns in `jobs` become insufficient for richer lineage, add:

- `from_job_id`
- `to_job_id`
- `link_kind`

This is optional for v1. Parent/root/retry columns are enough initially.

## 10. Redis Design

Redis should be used for:

- queueing jobs;
- transient cancellation fanout;
- WebSocket notification fanout;
- short-lived locks if needed.

Redis should not be used as durable job state storage.

### 10.1 Queues

Recommended queues:

- `jobs.transcription`
- `jobs.report`
- `jobs.deep_research`

### 10.2 Pub/Sub Channels

Recommended channels:

- `job-events`
- `job-cancel`

### 10.3 Queue Payload

Minimal queue message:

```json
{
  "job_id": "uuid",
  "job_type": "transcription",
  "attempt": 1
}
```

Do not place full job metadata in Redis. Workers must read authoritative state from API or DB-facing internal layer.

## 11. MinIO Storage Model

### 11.1 Buckets

Recommended buckets:

- `sources`
- `artifacts`

### 11.2 Object Key Conventions

Recommended object paths:

```text
sources/{source_id}/original/{filename}
artifacts/{job_id}/transcript/plain/transcript.txt
artifacts/{job_id}/transcript/segmented/transcript.md
artifacts/{job_id}/transcript/docx/transcript.docx
artifacts/{job_id}/report/markdown/report.md
artifacts/{job_id}/report/docx/report.docx
artifacts/{job_id}/deep-research/markdown/evidence-research-final-report.md
artifacts/{job_id}/logs/execution.log
```

### 11.3 Access Model

Clients must not upload directly to MinIO in v1.

Instead:

- upload to API;
- API stores source in MinIO;
- workers write artifacts to MinIO;
- clients download via API artifact endpoint.

This keeps the client surface smaller and simplifies local deployment.

## 12. HTTP API Contract

This section describes the recommended v1 contract. The exact OpenAPI spec should be authored from this section.

### 12.1 Create Transcription Job from Upload

`POST /v1/transcription-jobs`

Content type:

- `multipart/form-data`

Fields:

- `file` binary required
- `display_name` optional
- `client_ref` optional

Response:

- `202 Accepted`

Response body:

```json
{
  "job": {
    "id": "job_uuid",
    "root_job_id": "job_uuid",
    "parent_job_id": null,
    "retry_of_job_id": null,
    "job_type": "transcription",
    "status": "queued",
    "version": 1,
    "progress_stage": "queued",
    "progress_message": "Waiting for worker",
    "source": {
      "id": "source_uuid",
      "source_kind": "uploaded_file",
      "display_name": "call.ogg"
    },
    "artifacts": [],
    "children": {
      "report_job_ids": [],
      "deep_research_job_ids": []
    },
    "created_at": "2026-04-19T12:00:00Z",
    "started_at": null,
    "finished_at": null
  }
}
```

### 12.2 Create Transcription Job from YouTube URL

`POST /v1/transcription-jobs/from-url`

Request:

```json
{
  "source_kind": "youtube_url",
  "url": "https://youtu.be/example",
  "display_name": "YouTube: example"
}
```

Response:

- `202 Accepted`
- same job envelope as upload flow

### 12.3 Get Job

`GET /v1/jobs/{job_id}`

Response:

```json
{
  "job": {
    "id": "job_uuid",
    "root_job_id": "job_uuid",
    "parent_job_id": null,
    "retry_of_job_id": null,
    "job_type": "transcription",
    "status": "succeeded",
    "version": 5,
    "progress_stage": "completed",
    "progress_message": "Transcript ready",
    "source": {
      "id": "source_uuid",
      "source_kind": "uploaded_file",
      "display_name": "call.ogg"
    },
    "artifacts": [
      {
        "id": "artifact_uuid",
        "artifact_kind": "transcript_plain",
        "format": "txt",
        "filename": "transcript.txt",
        "mime_type": "text/plain",
        "download_url": "/v1/artifacts/artifact_uuid/download"
      }
    ],
    "children": {
      "report_job_ids": ["report_job_uuid"],
      "deep_research_job_ids": []
    },
    "created_at": "2026-04-19T12:00:00Z",
    "started_at": "2026-04-19T12:00:03Z",
    "finished_at": "2026-04-19T12:02:30Z"
  }
}
```

### 12.4 List Jobs

`GET /v1/jobs`

Supported query params:

- `status`
- `job_type`
- `root_job_id`
- `page`
- `page_size`

Default ordering:

- `created_at desc`

### 12.5 Create Report Job

`POST /v1/transcription-jobs/{job_id}/report-jobs`

Request:

```json
{
  "prompt_suffix": ""
}
```

Response:

- `202 Accepted`
- returns newly created report job

Preconditions:

- transcription job must be `succeeded`
- required transcript artifacts must exist

### 12.6 Create Deep Research Job

`POST /v1/report-jobs/{job_id}/deep-research-jobs`

Response:

- `202 Accepted`
- returns newly created deep research job

Preconditions:

- report job must be `succeeded`
- required report artifact must exist

### 12.7 Cancel Job

`POST /v1/jobs/{job_id}/cancel`

Response:

- `202 Accepted`

Behavior:

- if job is `queued`, API may switch directly to `canceled` and remove queue item if implementation supports it;
- if job is `running`, API sets `cancel_requested` and worker must observe it cooperatively;
- if job is terminal, API returns current job unchanged.

### 12.8 Retry Job

`POST /v1/jobs/{job_id}/retry`

Response:

- `202 Accepted`
- returns a new job with `retry_of_job_id = original_job_id`

Retry must create a new job. It must not reuse the same job row.

### 12.9 Download Artifact

`GET /v1/artifacts/{artifact_id}/download`

Recommended v1 behavior:

- API proxies or streams artifact from MinIO
- client receives file directly from API

Do not require clients to know MinIO bucket or object keys.

### 12.10 Job Events

`GET /v1/jobs/{job_id}/events`

Optional in first delivery, but recommended.

Useful for:

- UI event timeline
- debugging worker progress
- richer MCP introspection

## 13. WebSocket Contract

### 13.1 Endpoint

`GET /v1/ws`

### 13.2 Subscription Model

Recommended v1 model:

- client connects;
- client may receive all events;
- UI filters client-side for local deployment.

If tighter filtering is desired in v1, allow subscribe message:

```json
{
  "action": "subscribe",
  "job_ids": ["job_uuid_1", "job_uuid_2"]
}
```

### 13.3 Event Envelope

Recommended envelope:

```json
{
  "event_id": "evt_uuid",
  "event_type": "job.updated",
  "job_id": "job_uuid",
  "root_job_id": "root_job_uuid",
  "version": 7,
  "emitted_at": "2026-04-19T12:00:00Z",
  "payload": {
    "status": "running",
    "progress_stage": "transcribing",
    "progress_message": "Processing audio chunks"
  }
}
```

Recommended event types:

- `job.created`
- `job.updated`
- `job.artifact_created`
- `job.completed`
- `job.failed`
- `job.canceled`

### 13.4 Polling vs WebSocket Rule

This rule is mandatory:

- polling is the correctness path;
- WebSocket is a latency optimization;
- if UI detects version gaps or reconnects, it must re-fetch job snapshots through REST.

## 14. Worker Contract

Workers should not read queue payload and execute blindly. They should perform a strict claim-and-run sequence.

Recommended worker lifecycle:

1. receive queue message with `job_id`;
2. ask API or internal service layer to claim the job;
3. if claim fails because job is already terminal or owned, stop;
4. mark job `running`;
5. emit progress events;
6. execute the work;
7. upload artifacts to MinIO;
8. report artifact metadata;
9. finalize job as `succeeded`, `failed`, or `canceled`.

### 14.1 Transcription Worker

Reused logic sources:

- current `transcribers.py`
- transcript rendering in `documents.py`
- source materialization logic from `service.py`

Outputs:

- `transcript_plain`
- `transcript_segmented_markdown`
- `transcript_docx`

### 14.2 Report Worker

Reused logic sources:

- current `cglm_runner.py`
- report docx rendering in `documents.py`

Outputs:

- `report_markdown`
- `report_docx`

### 14.3 Deep Research Worker

Reused logic sources:

- current `deep_research.py`

Outputs:

- `deep_research_markdown`
- optional execution log artifact

## 15. Cancellation Semantics

Cancellation must be planned from day one.

### 15.1 API Rules

When cancel is requested:

- set `cancel_requested_at`;
- if running, set job status `cancel_requested`;
- emit event.

### 15.2 Worker Rules

Workers must check cancellation:

- before expensive initialization;
- between major phases;
- after subprocess completion;
- before artifact upload if the operation can still be skipped;
- inside loops if processing chunked work.

### 15.3 Subprocess Handling

For `cglm` and deep research:

- subprocess handle must be tracked;
- if cancel is observed, worker should terminate subprocess;
- if subprocess termination succeeds, worker finalizes as `canceled`;
- if subprocess crashes during cancel, worker still reports `canceled` if cancel was authoritative, unless data corruption requires `failed`.

## 16. Retry Semantics

Retry must create a new job.

Rules:

- keep original job immutable once terminal;
- new job references old via `retry_of_job_id`;
- parent/root lineage remains explicit;
- retry should reuse the original `source_id` when possible;
- retry should not duplicate source upload unless necessary.

This model is cleaner for:

- UI history;
- debugging;
- MCP introspection;
- event auditability.

## 17. Client Responsibilities

### 17.1 Web UI

Responsibilities:

- create jobs;
- list and inspect jobs;
- subscribe to WS and fallback to polling;
- trigger report and deep research child jobs;
- cancel and retry jobs;
- download artifacts.

### 17.2 Telegram Bot

Responsibilities:

- receive Telegram user input;
- download attachment from Telegram if needed;
- upload file to API;
- create URL jobs through API;
- poll or subscribe for result readiness;
- send resulting artifacts back to Telegram user;
- expose buttons that trigger API actions.

It must not:

- run transcription locally;
- generate reports locally;
- write direct job metadata;
- own artifact lifecycle.

### 17.3 MCP Server

Responsibilities:

- wrap API as MCP tools;
- expose job create/read/list/retry/cancel/report/deep research operations;
- return artifact metadata and download handles.

It must not:

- contain execution logic;
- talk directly to Redis, MinIO, or workers.

## 18. React UI Requirements

The web UI is a simple operational app, not a product suite.

Required pages:

- `Create Job`
- `Jobs List`
- `Job Details`

Required capabilities:

- upload file;
- submit YouTube URL;
- observe live state;
- view artifact list;
- download artifacts;
- create report job;
- create deep research job;
- cancel job;
- retry job.

Recommended stack:

- React
- TypeScript
- Vite
- TanStack Query
- React Router

## 19. MCP Server Requirements

Recommended MCP tools:

- `create_transcription_job_from_file`
- `create_transcription_job_from_url`
- `get_job`
- `list_jobs`
- `create_report_job`
- `create_deep_research_job`
- `cancel_job`
- `retry_job`
- `list_job_artifacts`
- `get_artifact_download_info`

Recommended stack:

- TypeScript
- official MCP SDK
- generated or thin typed API client from `packages/sdk-ts`

## 20. Telegram Bot Requirements

Recommended stack:

- Python
- aiogram
- `packages/sdk-py` or thin internal API client

Behavior mapping:

- upload media -> `POST /v1/transcription-jobs`
- YouTube URL -> `POST /v1/transcription-jobs/from-url`
- plain transcript ready -> send plain transcript artifact
- segmented transcript button -> request job and send segmented artifact
- report button -> create report job and wait/poll
- deep research button -> create deep research job and wait/poll

## 21. Docker Compose Design

### 21.1 Required Services

`postgres`

- local persistent volume
- init DB

`redis`

- no persistence requirement beyond local convenience

`minio`

- local persistent volume
- exposed admin console optional

`minio-init`

- creates required buckets:
  - `sources`
  - `artifacts`

`api`

- depends on postgres, redis, minio
- exposes HTTP + WS

`worker-transcription`

- depends on api, redis, minio
- includes ffmpeg and faster-whisper

`worker-report`

- depends on api, redis, minio

`worker-deep-research`

- depends on api, redis, minio

`web`

- depends on api

`telegram-bot`

- depends on api

`mcp-server`

- depends on api

### 21.2 Volumes

Recommended persistent volumes:

- `postgres-data`
- `minio-data`
- `whisper-model-cache`

### 21.3 CPU Resource Notes

For local deployment:

- do not start multiple transcription workers by default if CPU is constrained;
- keep report and deep research workers separate so they do not block transcription queue;
- surface long-running progress in API and WS so UI does not appear frozen.

## 22. Environment Variables

### 22.1 Shared Infrastructure

- `POSTGRES_DSN`
- `REDIS_URL`
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET_SOURCES`
- `MINIO_BUCKET_ARTIFACTS`

### 22.2 API

- `API_BIND_ADDR`
- `API_BASE_URL`
- `WS_BASE_URL`

### 22.3 Transcription Worker

- `WHISPER_MODEL`
- `WHISPER_DEVICE=cpu`
- `WHISPER_COMPUTE_TYPE=default`
- `FFMPEG_BIN`

### 22.4 Report Worker

- `CGLM_BIN`
- `REPORT_PROMPT_SUFFIX`

### 22.5 Deep Research Worker

- `CLAUDE_BIN`
- `DEEP_RESEARCH_MODEL`

### 22.6 Telegram Bot

- `TELEGRAM_BOT_TOKEN`
- `API_BASE_URL`

### 22.7 Web

- `VITE_API_BASE_URL`
- `VITE_WS_URL`

### 22.8 MCP Server

- `API_BASE_URL`

## 23. Migration Mapping from Current Files

This section maps the current code into future destinations.

### 23.1 `src/telegram_transcriber_bot/transcribers.py`

Move into:

- `workers/common/src/transcriber_workers_common/transcribers.py`
- `workers/transcription/src/...`

### 23.2 `src/telegram_transcriber_bot/documents.py`

Move into:

- `workers/common/src/transcriber_workers_common/documents.py`

### 23.3 `src/telegram_transcriber_bot/cglm_runner.py`

Move into:

- `workers/report/src/...`

### 23.4 `src/telegram_transcriber_bot/deep_research.py`

Move into:

- `workers/deep-research/src/...`

### 23.5 `src/telegram_transcriber_bot/service.py`

Split:

- execution-specific code into Python workers;
- job orchestration semantics into Go API service layer;
- metadata persistence logic replaced by PostgreSQL + MinIO + Redis integration.

### 23.6 `src/telegram_transcriber_bot/bot.py`

Keep only Telegram adapter concerns:

- message parsing;
- Telegram downloads;
- client-side polling or wait flow;
- result delivery;
- callback-to-API mapping.

Move out:

- direct execution;
- local job state persistence;
- direct report generation;
- deep research execution.

## 24. Implementation Waves

This section is the execution backbone of the migration.

The recommendation is to execute waves in order and not mix cutover work too early.

### Wave 0: Baseline Freeze and Acceptance SSOT

**Goal:** Freeze the current behavior so migration has a hard acceptance target.

**Deliverables:**

- migration baseline doc
- behavior matrix
- artifact matrix
- current runtime verification notes

**Files:**

- Create: `docs/architecture/current-system-baseline.md`
- Create: `docs/architecture/current-behavior-matrix.md`
- Modify: `README.md`

**Phases:**

#### Phase 0.1: Record current supported flows

Document:

- YouTube
- Telegram voice
- Telegram audio/video/document
- media groups
- plain transcript
- segmented transcript
- report
- deep research

#### Phase 0.2: Record current artifact names and meanings

Document current artifacts:

- `transcript.txt`
- `transcript.md`
- `transcript.docx`
- `report.md`
- `report.docx`
- deep research final report

#### Phase 0.3: Freeze acceptance tests

Tag which tests must remain conceptually valid after migration.

### Wave 1: Monorepo Bootstrap and Contracts

**Goal:** Create monorepo structure and contract package before implementation spreads.

**Files:**

- Create: `apps/api/...`
- Create: `apps/web/...`
- Create: `apps/telegram-bot/...`
- Create: `apps/mcp-server/...`
- Create: `workers/...`
- Create: `packages/contracts/...`
- Create: `packages/sdk-ts/...`
- Create: `packages/sdk-py/...`
- Create: `infra/docker-compose.yml`

**Phases:**

#### Phase 1.1: Create repository structure

Create all top-level directories and placeholder READMEs.

#### Phase 1.2: Write OpenAPI draft

Initial contract must include:

- create transcription job
- get job
- list jobs
- create report job
- create deep research job
- cancel
- retry
- artifact download

#### Phase 1.3: Write WS schema draft

Define event envelope and event types.

#### Phase 1.4: Define shared enums

Define:

- job types
- statuses
- artifact kinds
- source kinds

### Wave 2: Infrastructure and Go API Skeleton

**Goal:** Bring up local infra and a compilable API skeleton with DB and queue wiring.

**Files:**

- Create: `apps/api/cmd/api/main.go`
- Create: `apps/api/internal/api/...`
- Create: `apps/api/internal/db/...`
- Create: `apps/api/internal/jobs/...`
- Create: `apps/api/internal/queue/...`
- Create: `apps/api/internal/storage/...`
- Create: `apps/api/internal/ws/...`
- Create: `apps/api/migrations/*.sql`
- Create: `infra/docker-compose.yml`
- Create: `infra/env/api.env.example`

**Phases:**

#### Phase 2.1: Add PostgreSQL migrations

Create tables:

- `sources`
- `jobs`
- `job_artifacts`
- `job_events`

#### Phase 2.2: Add MinIO integration

Implement bucket bootstrap assumptions and object put/get wrappers.

#### Phase 2.3: Add Redis queue producer

Implement enqueue functions by job type.

#### Phase 2.4: Add REST endpoints

Implement stubs first, then persistence-backed handlers.

#### Phase 2.5: Add WebSocket server

Implement WS connection, event broadcast, and reconnect-safe snapshot rule.

### Wave 3: Transcription Worker Extraction

**Goal:** Move transcription execution out of the legacy bot into a standalone worker.

**Files:**

- Create: `workers/common/src/transcriber_workers_common/...`
- Create: `workers/transcription/src/...`
- Create: `workers/transcription/tests/...`
- Modify: current Python modules only as needed for extraction

**Phases:**

#### Phase 3.1: Extract reusable Python execution library

Move transcription and document rendering logic into worker-common package.

#### Phase 3.2: Implement worker claim/run loop

Worker must:

- receive `job_id`
- fetch source metadata
- download source from MinIO if needed
- execute transcription
- upload artifacts
- publish progress
- finalize job

#### Phase 3.3: Preserve current output formats

The new worker must reproduce:

- plain transcript
- segmented markdown transcript
- transcript docx

#### Phase 3.4: Cancellation integration

Check cancel flag before and after long work.

### Wave 4: Report Worker Extraction

**Goal:** Split report generation into a dedicated job type and worker.

**Files:**

- Create: `workers/report/src/...`
- Create: `workers/report/tests/...`

**Phases:**

#### Phase 4.1: Extract report generation package

Move `cglm_runner` and report DOCX generation into report worker layer.

#### Phase 4.2: Implement report job creation in API

API must validate that parent transcription artifacts exist.

#### Phase 4.3: Implement report worker lifecycle

Worker must:

- claim report job
- read transcript artifact
- run cglm
- upload markdown and docx
- finalize job

### Wave 5: Deep Research Worker Extraction

**Goal:** Split deep research into a dedicated job type and worker.

**Files:**

- Create: `workers/deep-research/src/...`
- Create: `workers/deep-research/tests/...`

**Phases:**

#### Phase 5.1: Extract deep research package

Move current deep research pipeline into worker package with minimal rewrite.

#### Phase 5.2: Implement API deep research child-job creation

API must validate report artifact existence.

#### Phase 5.3: Implement cooperative cancellation

Deep research worker must observe cancel before each phase and around subprocess execution.

### Wave 6: Web UI Delivery

**Goal:** Deliver a simple operational React UI over the live API.

**Files:**

- Create: `apps/web/src/app/...`
- Create: `apps/web/src/features/jobs/...`
- Create: `apps/web/src/features/artifacts/...`
- Create: `apps/web/src/lib/api/...`

**Phases:**

#### Phase 6.1: Scaffold app shell

Create:

- routes
- layout
- API client wiring

#### Phase 6.2: Create jobs list page

Display:

- job type
- status
- created time
- progress

#### Phase 6.3: Create job details page

Display:

- status
- progress
- event timeline
- artifacts
- child jobs

#### Phase 6.4: Add create job form

Support:

- file upload
- YouTube URL submission

#### Phase 6.5: Add polling and WebSocket integration

Use polling as fallback and WS for fast updates.

#### Phase 6.6: Add actions

- report
- deep research
- cancel
- retry

### Wave 7: Telegram Bot Migration

**Goal:** Replace direct execution in Telegram bot with API calls.

**Files:**

- Create: `apps/telegram-bot/src/...`
- Create: `packages/sdk-py/...`
- Retire logic from current `src/telegram_transcriber_bot/service.py` usage

**Phases:**

#### Phase 7.1: Add Python API client

Support:

- create upload job
- create URL job
- get job
- create report job
- create deep research job
- cancel/retry optional if bot exposes them

#### Phase 7.2: Adapt message flow

Bot becomes:

- input parser
- Telegram downloader
- HTTP client
- artifact sender

#### Phase 7.3: Preserve current UX

Keep:

- plain transcript immediate delivery
- segmented transcript button
- report button
- deep research button

#### Phase 7.4: Remove execution coupling

Bot must no longer import execution core directly.

### Wave 8: MCP Server Delivery

**Goal:** Expose the platform to MCP as a clean adapter.

**Files:**

- Create: `apps/mcp-server/src/index.ts`
- Create: `apps/mcp-server/src/tools/...`
- Reuse: `packages/sdk-ts`

**Phases:**

#### Phase 8.1: Scaffold MCP server

#### Phase 8.2: Add core tools

- create transcription job
- get job
- list jobs
- create report job
- create deep research job
- cancel
- retry

#### Phase 8.3: Add artifact visibility

Return artifact metadata and download endpoints.

### Wave 9: Compose Hardening and Cutover

**Goal:** Make the new platform the default local runtime.

**Files:**

- Modify: `infra/docker-compose.yml`
- Modify: `README.md`
- Create: `docs/architecture/cutover-checklist.md`
- Create: `docs/architecture/runtime-ops.md`

**Phases:**

#### Phase 9.1: Add persistent volumes and init scripts

#### Phase 9.2: Add healthchecks

#### Phase 9.3: Add local runbook

Document:

- startup
- restart
- logs
- common failures

#### Phase 9.4: Cut over legacy entrypoint

Decide whether legacy bot entrypoint becomes:

- archived
- compatibility wrapper
- removed after verified parity

## 25. Testing and Verification Strategy

### 25.1 Contract Tests

Required:

- OpenAPI schema validation
- JSON schema tests for WS events
- SDK compatibility tests

### 25.2 API Integration Tests

Required:

- create transcription job
- create report job
- create deep research job
- cancel
- retry
- artifact download

### 25.3 Worker Integration Tests

Required:

- transcription worker end-to-end against MinIO + Postgres + Redis
- report worker integration with report artifact output
- deep research worker integration with phase output checks

### 25.4 UI Tests

Required:

- upload file
- create URL job
- observe live transition via polling
- artifact download flow
- report and deep research triggers
- cancel and retry buttons

### 25.5 Telegram Bot Tests

Required:

- upload flow through API mock
- URL flow through API mock
- button callbacks map to API actions
- plain transcript delivery remains first-class

### 25.6 MCP Tests

Required:

- tool to API mapping
- artifact metadata responses
- error propagation

## 26. Risks and Mitigations

### Risk 1: Rewriting too much execution logic

Mitigation:

- keep Python execution plane;
- move code, do not rewrite for style;
- introduce wrappers, not reinventions.

### Risk 2: Premature Go rewrite of ML/report logic

Mitigation:

- Go owns control plane only;
- workers stay Python.

### Risk 3: WebSocket state drift

Mitigation:

- polling remains authoritative fallback;
- use monotonic `version` field on jobs.

### Risk 4: Cancellation is inconsistent across job types

Mitigation:

- define cooperative cancellation checkpoints per worker;
- test subprocess termination behavior explicitly.

### Risk 5: Compose stack becomes fragile

Mitigation:

- add healthchecks;
- add startup order;
- document runbook;
- keep local deployment assumptions simple.

### Risk 6: Legacy bot and new bot diverge during migration

Mitigation:

- freeze behavior baseline early;
- move UX only after API workers are already correct.

## 27. Definition of Done

The migration is complete only when all of the following are true:

- API server owns all business logic orchestration.
- Transcription, report, and deep research run only in workers.
- Telegram bot is a thin API client.
- MCP server is a thin API client.
- Web UI can create, observe, and manage jobs.
- PostgreSQL is authoritative job state storage.
- MinIO stores source files and artifacts.
- Redis is used only for queueing and event fanout.
- Polling and WebSocket are both implemented.
- Retry creates new jobs.
- Cancellation works across all job types.
- Docker Compose can bring up the full platform locally.
- Current core capabilities are preserved.

## 28. Immediate Next Task Recommendation

Implementation should start with Wave 0 and Wave 1 only.

Do not begin with UI or Telegram refactor first.

The recommended first coding order is:

1. create `packages/contracts`
2. bootstrap `apps/api`
3. add DB schema and queue producer
4. add transcription worker
5. verify end-to-end transcription job path
6. only then add UI and adapter clients

## 29. Execution Rule for Future Sessions

When implementing this plan:

- do not start from the UI;
- do not start from the MCP server;
- do not rewrite Python execution into Go;
- do not add auth or quota logic;
- do not build direct client-to-MinIO uploads in v1;
- do not use WebSocket as the only status mechanism;
- do not implement retry by mutating a finished job row.

The migration should always preserve the separation:

- Go API = control plane
- Python workers = execution plane
- UI/Bot/MCP = adapters

