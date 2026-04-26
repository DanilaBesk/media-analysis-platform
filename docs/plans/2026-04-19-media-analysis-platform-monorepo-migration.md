# Media Analysis Platform Monorepo Migration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Preserve the migration record from the former single-process transcription bot into `media-analysis-platform`: a local-first monorepo platform with a Go API control plane, Python execution workers, React web UI, thin Telegram adapter, separate MCP server, PostgreSQL state, MinIO artifact storage, Docker Compose orchestration, and first-class polling, WebSocket, webhook, retry, and cancellation contracts.

**Architecture:** The target system separates orchestration from execution. The Go API server owns job creation, state transitions, artifact metadata, job graph relationships, polling/WS/webhook status delivery, idempotency, and cancel/retry semantics. Python workers own heavy execution for transcription, report generation, and deep research by reusing the existing Python pipeline. Telegram bot, web UI, and MCP become thin adapters over the same HTTP API.

**Tech Stack:** Go API server (`chi`, `pgx`, `sqlc`, `goose`, `asynq`), PostgreSQL, Redis, MinIO, React + TypeScript web app, Python workers, Python Telegram bot, TypeScript MCP server, Docker Compose, CPU-only faster-whisper in Docker.

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

The current repository is a compose-owned local platform with one host-side cutover entrypoint:

- [src/media_analysis_platform/__main__.py](/Users/danila/work/my/media-analysis-platform/src/media_analysis_platform/__main__.py:1)

The current logic is distributed as follows:

- [src/media_analysis_platform/bot.py](/Users/danila/work/my/media-analysis-platform/src/media_analysis_platform/bot.py:1)
  Telegram runtime, source extraction orchestration, attachment download, media group buffering, callback handlers, status messages, and result delivery.
- [workers/transcription/src/transcriber_worker_transcription.py](/Users/danila/work/my/media-analysis-platform/workers/transcription/src/transcriber_worker_transcription.py:1)
  Transcription worker runtime, local source materialization, ordered input handling, and transcript artifact persistence.
- [workers/common/src/transcriber_workers_common/transcribers.py](/Users/danila/work/my/media-analysis-platform/workers/common/src/transcriber_workers_common/transcribers.py:1)
  Shared YouTube transcript fast path and Whisper fallback.
- [workers/common/src/transcriber_workers_common/documents.py](/Users/danila/work/my/media-analysis-platform/workers/common/src/transcriber_workers_common/documents.py:1)
  Transcript/report markdown normalization and DOCX artifact rendering.
- [workers/agent-runner/src/transcriber_worker_agent_runner.py](/Users/danila/work/my/media-analysis-platform/workers/agent-runner/src/transcriber_worker_agent_runner.py:1)
  Generic `agent_run` worker runtime for report and deep-research execution.

Current storage and runtime model:

- PostgreSQL stores jobs, events, source sets, and artifact metadata
- MinIO stores uploaded sources and produced artifacts
- Redis backs asynq queues only
- Go HTTP API owns orchestration and delivery semantics
- React Web UI, Telegram adapter, and MCP adapter consume the API boundary
- host-side `uv run media-analysis-platform` is only a cutover pointer, not the old execution path

Current user-visible capabilities to preserve:

- YouTube transcription
- Telegram voice/audio/video/document transcription
- media group handling
- plain transcript output
- segmented transcript output
- report generation
- deep research generation

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

## 4.1 Frozen Design Decisions

The following decisions are fixed for this migration unless explicitly superseded by a later architectural decision document.

1. The API server is implemented in Go and acts as the sole control plane.
2. Heavy execution remains in Python workers.
3. Queueing is implemented with `asynq`, not Redis Streams or Redis lists.
4. Workers must not perform direct business-state mutations with arbitrary SQL; they interact through the control-plane contract or tightly scoped internal service layer owned by the API.
5. Create endpoints support optional `Idempotency-Key`.
6. Repeated attempts to create child jobs for the same parent return the existing active or completed child job unless the caller explicitly requests retry.
7. Progress is stage-based and message-based; fake percentages are forbidden.
8. Cancellation is cooperative and phase-aware.
9. Retry always creates a new job.
10. Runtime advanced knobs are not exposed in Web UI or Telegram bot in v1.
11. Artifact downloads are returned as MinIO presigned links in v1 for local deployment.
12. Every job chooses exactly one delivery strategy: `polling` or `webhook`.
13. Default delivery strategy is `polling`.
14. The API always returns `job_id` and an initial job snapshot, regardless of delivery strategy.
15. `webhook` is supported for all job types and requires a callback URL.
16. WebSocket is the live-update channel for the web UI.
17. First-version file uploads go through the API and support multiple files in one request.
18. There is no legacy compatibility track. The repository is restructured directly toward the target architecture, with a full-system rewrite and cutover.

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
- `webhook delivery subsystem`
  Outbound callback delivery owned by the API server for asynchronous job notifications.

### 5.2 Control Plane vs Execution Plane

**Control plane responsibilities**

- accept uploads and source creation requests;
- create jobs and child jobs;
- resolve delivery strategy (`polling` or `webhook`) for create requests;
- persist job state in PostgreSQL;
- store source files in MinIO;
- enqueue work in `asynq`;
- stream job updates via WebSocket;
- deliver outbound webhooks on job lifecycle changes;
- expose polling endpoints;
- expose artifact metadata and MinIO presigned download links;
- enforce idempotency and child-job reuse semantics;
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
│   │   └── internal/storage/migrations
│   ├── web
│   │   ├── src
│   │   │   ├── app
│   │   │   ├── features/jobs
│   │   │   ├── features/artifacts
│   │   │   └── lib/api
│   ├── telegram-bot
│   │   ├── src/telegram_adapter
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
```

Notes:

- The repository is restructured directly into the target monorepo shape.
- There is no long-lived `legacy/` subtree and no compatibility branch inside the target structure.
- Existing Python modules may temporarily remain in place only while being actively moved into target packages during early waves, but the plan assumes direct replacement rather than prolonged coexistence.

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

Each job type supports both callback modes:

- `polling`
- `webhook`

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

### 8.5 Delivery Strategy

The system must support explicit delivery strategy selection per create request.

Recommended enum:

- `polling`
- `webhook`

Rules:

- each job uses exactly one delivery strategy;
- default strategy is `polling`;
- `webhook` can be requested at job creation;
- `webhook` applies to all job types;
- the API always returns `job_id` and an initial snapshot even when `webhook` is selected;
- polling endpoints remain available as a recovery/read model even for webhook-configured jobs, but they are not the primary delivery mode in that case.

## 9. PostgreSQL Schema

The first version does not need a broad domain model. It does need an operational schema that cleanly represents:

- request idempotency;
- source objects;
- job lineage;
- artifact metadata;
- timeline events;
- webhook delivery retries that survive API restarts.

Recommended enums:

- `source_kind`: `uploaded_file`, `telegram_upload`, `youtube_url`
- `job_type`: `transcription`, `report`, `deep_research`
- `job_status`: `queued`, `running`, `cancel_requested`, `succeeded`, `failed`, `canceled`
- `delivery_strategy`: `polling`, `webhook`
- `artifact_kind`: `transcript_plain`, `transcript_segmented_markdown`, `transcript_docx`, `report_markdown`, `report_docx`, `deep_research_markdown`, `execution_log`
- `submission_kind`: `transcription_upload`, `transcription_url`, `report_create`, `deep_research_create`, `job_retry`

### 9.1 Table: `job_submissions`

This table is required in v1 because `Idempotency-Key` plus multi-file upload cannot be represented safely on `jobs` alone.

Suggested columns:

- `id uuid primary key`
- `submission_kind text not null`
- `idempotency_key text not null`
- `request_fingerprint text not null`
- `created_at timestamptz not null default now()`

Constraints and indexes:

- `unique (submission_kind, idempotency_key)`
- index `(created_at desc)`

Rules:

- one submission may create many jobs for `POST /v1/transcription-jobs`;
- same submission key plus same normalized request returns the original accepted response;
- same submission key plus different normalized request returns `409 idempotency_conflict`.

### 9.2 Table: `sources`

Suggested columns:

- `id uuid primary key`
- `source_kind text not null`
- `display_name text not null`
- `original_filename text null`
- `mime_type text null`
- `source_url text null`
- `object_key text null`
- `size_bytes bigint null`
- `created_at timestamptz not null default now()`

Constraints and indexes:

- `check (source_kind = 'youtube_url' and source_url is not null and object_key is null) or (source_kind in ('uploaded_file', 'telegram_upload') and object_key is not null)`
- `unique (object_key) where object_key is not null`
- index `(source_kind, created_at desc)`

Rules:

- `youtube_url` sources do not need an object in MinIO in v1;
- uploaded and Telegram sources must have `object_key`;
- all jobs in one lineage reuse the same `source_id`.

### 9.3 Table: `jobs`

Suggested columns:

- `id uuid primary key`
- `submission_id uuid null references job_submissions(id)`
- `root_job_id uuid not null references jobs(id)`
- `parent_job_id uuid null references jobs(id)`
- `retry_of_job_id uuid null references jobs(id)`
- `source_id uuid not null references sources(id)`
- `job_type text not null`
- `status text not null`
- `delivery_strategy text not null default 'polling'`
- `webhook_url text null`
- `version bigint not null default 1`
- `progress_stage text not null default 'queued'`
- `progress_message text not null default ''`
- `params jsonb not null default '{}'::jsonb`
- `client_ref text null`
- `error_code text null`
- `error_message text null`
- `cancel_requested_at timestamptz null`
- `created_at timestamptz not null default now()`
- `started_at timestamptz null`
- `finished_at timestamptz null`

Constraints and indexes:

- `check (delivery_strategy = 'webhook' and webhook_url is not null) or (delivery_strategy = 'polling' and webhook_url is null)`
- `check (status <> 'cancel_requested' or cancel_requested_at is not null)`
- index `(root_job_id, created_at desc)`
- index `(parent_job_id, job_type, created_at desc)`
- index `(retry_of_job_id)`
- index `(source_id, created_at desc)`
- index `(job_type, status, created_at desc)`
- partial unique `(parent_job_id, job_type) where parent_job_id is not null and retry_of_job_id is null and status in ('queued', 'running', 'cancel_requested', 'succeeded')`

Rules:

- initial transcription job: `root_job_id = id`, `parent_job_id = null`, `retry_of_job_id = null`;
- retry job keeps the same `root_job_id`, keeps the same `parent_job_id`, and sets `retry_of_job_id`;
- child report jobs point to a transcription parent;
- child deep research jobs point to a report parent;
- v1 should keep only one canonical non-retry child per parent/job type; reruns happen only through retry rows.

### 9.4 Table: `job_artifacts`

Suggested columns:

- `id uuid primary key`
- `job_id uuid not null references jobs(id) on delete cascade`
- `artifact_kind text not null`
- `filename text not null`
- `format text not null`
- `mime_type text not null`
- `object_key text not null`
- `size_bytes bigint null`
- `created_at timestamptz not null default now()`

Constraints and indexes:

- `unique (job_id, artifact_kind)`
- `unique (object_key)`
- index `(job_id, created_at asc)`

Rules:

- do not model `source_original` as an artifact in v1; uploaded originals already live in `sources`;
- create row only after object upload succeeds;
- successful jobs must satisfy required artifact sets:
  - transcription: `transcript_plain`, `transcript_segmented_markdown`, `transcript_docx`
  - report: `report_markdown`, `report_docx`
  - deep research: `deep_research_markdown`

### 9.5 Table: `job_events`

Suggested columns:

- `id uuid primary key`
- `job_id uuid not null references jobs(id) on delete cascade`
- `root_job_id uuid not null references jobs(id)`
- `event_type text not null`
- `version bigint not null`
- `payload jsonb not null`
- `created_at timestamptz not null default now()`

Constraints and indexes:

- `unique (job_id, version)`
- index `(job_id, created_at asc)`
- index `(root_job_id, created_at asc)`

Rules:

- timeline events are append-only;
- one job version should map to one persisted event row;
- webhook delivery records should reference `job_events`, not raw jobs, so replay and dedupe stay deterministic.

### 9.6 Table: `webhook_deliveries`

This table is required in v1 because bounded webhook retry must survive API restart.

Suggested columns:

- `id uuid primary key`
- `job_event_id uuid not null unique references job_events(id) on delete cascade`
- `job_id uuid not null references jobs(id) on delete cascade`
- `target_url text not null`
- `payload jsonb not null`
- `state text not null default 'pending'`
  Values:
  - `pending`
  - `delivered`
  - `dead`
- `attempt_count int not null default 0`
- `next_attempt_at timestamptz not null default now()`
- `last_attempt_at timestamptz null`
- `delivered_at timestamptz null`
- `last_http_status int null`
- `last_error text null`
- `created_at timestamptz not null default now()`

Indexes:

- `(state, next_attempt_at)`
- `(job_id, created_at asc)`

Rules:

- create one row per emitted event when `delivery_strategy = webhook`;
- no separate per-attempt history table is required in v1;
- `attempt_count` plus last outcome is enough operationally.

## 10. Redis and Asynq Design

Redis is the backing infrastructure for:

- `asynq` queues;
- transient cancellation fanout if needed;
- WebSocket notification fanout if needed;
- short-lived coordination primitives.

Redis is not durable job state storage.

### 10.1 Queueing with Asynq

Queueing must be implemented with `asynq`.

Recommended queues:

- `transcription`
- `report`
- `deep_research`

Recommended task types:

- `job:transcription.run`
- `job:report.run`
- `job:deep_research.run`

Recommended timeouts and retry ceilings:

- `job:transcription.run`: `MaxRetry=3`, `Timeout=2h`
- `job:report.run`: `MaxRetry=2`, `Timeout=45m`
- `job:deep_research.run`: `MaxRetry=2`, `Timeout=2h`

### 10.2 Asynq Payload

Minimal task payload:

```json
{
  "job_id": "uuid",
  "attempt": 1
}
```

Do not place full job metadata in Redis or asynq payloads. Workers must resolve authoritative state through the API-owned contract.

### 10.3 Retry and Pending Ownership

The API remains the owner of business retry semantics.

Rules:

- `asynq` retries can be used for transient infrastructure failures;
- business-visible retry still creates a new job row;
- terminal job state transitions are owned by the control plane, not by queue metadata alone.
- deterministic business failures should use `SkipRetry`, not queue-level blind repetition.

Recommended `SkipRetry` cases:

- claim rejected because job is already terminal or already owned;
- cancel is already authoritative;
- required parent artifact is missing;
- request data is invalid;
- pipeline/model failure that should surface to the user as terminal `failed`.

## 11. MinIO Storage Model

### 11.1 Buckets

Recommended buckets:

- `sources`
- `artifacts`

### 11.2 Object Key Conventions

Recommended object paths:

```text
sources/{source_id}/original/{sanitized_filename}
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
- API stores source files in MinIO;
- workers write artifacts to MinIO;
- clients receive MinIO presigned download links from the API and download directly from those links.

This keeps writes centralized while making local downloads simple and fast.

For `youtube_url` sources, v1 may store only the database row and materialize the downloaded media inside workers or temporary runtime storage. An object in `sources` bucket is not mandatory for that source kind.

### 11.4 Presigned Download Links

Artifact responses should expose short-lived presigned links.

Recommended fields:

- `download_url`
- `download_url_expires_at`

Rules:

- presigned URLs are generated by the API at read time;
- clients must treat them as ephemeral;
- the job snapshot and artifact metadata remain available from the API even if the URL expires.

Recommended v1 object metadata:

- source objects:
  - `source-id`
  - `source-kind`
  - `original-filename`
- artifact objects:
  - `artifact-id`
  - `job-id`
  - `root-job-id`
  - `artifact-kind`

This metadata is convenience only. PostgreSQL remains authoritative.

## 12. HTTP API Contract

This section describes the recommended v1 contract. The exact OpenAPI spec should be authored from this section.

### 12.1 Common Request Rules

Common rules for create endpoints:

- support optional `Idempotency-Key` header;
- accept `delivery`;
- if `delivery.strategy=webhook`, require `delivery.webhook.url`;
- if an equivalent request with the same idempotency key is already accepted, return the existing job snapshot rather than creating a new one.
- no auth in v1;
- `retry` is intentionally not idempotent;
- child-job creation is stronger than idempotency: if a canonical child already exists, return it even without an idempotency key.

Recommended shared validation enums:

- `JobStatus`: `queued | running | cancel_requested | succeeded | failed | canceled`
- `JobType`: `transcription | report | deep_research`
- `SourceKind`: `uploaded_file | telegram_upload | youtube_url`
- `ArtifactKind`: `transcript_plain | transcript_segmented_markdown | transcript_docx | report_markdown | report_docx | deep_research_markdown | execution_log`

Recommended shape for JSON endpoints:

```json
{
  "delivery": {
    "strategy": "polling"
  }
}
```

Webhook variant:

```json
{
  "delivery": {
    "strategy": "webhook",
    "webhook": {
      "url": "http://host.docker.internal:8090/hooks/transcriber"
    }
  }
}
```

For `multipart/form-data` endpoints, the same contract should be represented as flattened fields:

- `delivery_strategy`
- `delivery_webhook_url`

Idempotency normalization rules:

- JSON requests normalize after applying default `delivery.strategy = polling`;
- multipart requests normalize scalar fields plus ordered file manifest:
  - `filename`
  - `size_bytes`
  - `sha256`
- same key and same normalized request returns the original `202` response body;
- same key and different normalized request returns `409 idempotency_conflict`.

Recommended error envelope:

```json
{
  "error": {
    "code": "string",
    "message": "string",
    "details": {}
  }
}
```

### 12.2 Create Transcription Job from Upload

`POST /v1/transcription-jobs`

Content type:

- `multipart/form-data`

Fields:

- `files` binary required, one or more files
- `display_name` optional, only valid when exactly one file is uploaded
- `client_ref` optional
- `delivery_strategy` optional, default `polling`
- `delivery_webhook_url` optional, required only when `delivery_strategy=webhook`

Response:

- `202 Accepted`

Recommended response:

- always return `jobs`, even for one file;
- single file means `jobs` contains exactly one snapshot.

Response body example:

```json
{
  "jobs": [
    {
      "id": "job_uuid",
      "root_job_id": "job_uuid",
      "parent_job_id": null,
      "retry_of_job_id": null,
      "job_type": "transcription",
      "status": "queued",
      "delivery": {
        "strategy": "polling"
      },
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
  ]
}
```

Behavior rules:

- multiple uploaded files create one transcription job per file;
- uploaded files must be persisted to MinIO before queueing work;
- the API may reject oversized uploads early;
- recommended first-version maximum request size is `1 GiB`.
- reject empty file parts;
- reject ambiguous `display_name` usage for multi-file upload.

### 12.3 Create Transcription Job from YouTube URL

`POST /v1/transcription-jobs/from-url`

Request:

```json
{
  "source_kind": "youtube_url",
  "url": "https://youtu.be/example",
  "display_name": "YouTube: example",
  "delivery": {
    "strategy": "polling"
  }
}
```

Response:

- `202 Accepted`
- same job envelope as upload flow

Validation rules:

- `source_kind` must be `youtube_url`;
- URL must be absolute `http` or `https`;
- allowed hosts in v1:
  - `youtube.com`
  - `www.youtube.com`
  - `m.youtube.com`
  - `youtu.be`

### 12.4 Get Job

`GET /v1/jobs/{job_id}`

Validation:

- `job_id` must be UUID.

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
    "delivery": {
      "strategy": "polling"
    },
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
        "download_url": "http://minio.local/presigned",
        "download_url_expires_at": "2026-04-19T12:07:30Z"
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

### 12.5 List Jobs

`GET /v1/jobs`

Supported query params:

- `status`
- `job_type`
- `root_job_id`
- `page`
- `page_size`

Default ordering:

- `created_at desc`

Validation rules:

- `status` must be valid enum if supplied;
- `job_type` must be valid enum if supplied;
- `root_job_id` must be UUID if supplied;
- `page >= 1`;
- `1 <= page_size <= 100`.

Recommended response shape:

```json
{
  "items": [],
  "page": 1,
  "page_size": 20,
  "has_more": false
}
```

### 12.6 Create Report Job

`POST /v1/transcription-jobs/{job_id}/report-jobs`

Request:

```json
{
  "delivery": {
    "strategy": "polling"
  },
  "prompt_suffix": ""
}
```

Response:

- `202 Accepted`
- returns newly created or already existing report job

Preconditions:

- transcription job must be `succeeded`
- required transcript artifacts must exist
- if a canonical child report job already exists for the same parent, return it instead of creating a duplicate
- a new report execution requires explicit retry if the previous child job is terminal and the caller wants a rerun

Validation rules:

- parent job must exist;
- parent `job_type` must be `transcription`;
- parent must contain `transcript_plain`;
- `prompt_suffix` may be optional string and should default to empty string.

### 12.7 Create Deep Research Job

`POST /v1/report-jobs/{job_id}/deep-research-jobs`

Request:

```json
{
  "delivery": {
    "strategy": "polling"
  }
}
```

Response:

- `202 Accepted`
- returns newly created or already existing deep research job

Preconditions:

- report job must be `succeeded`
- required report artifact must exist
- if a canonical child deep research job already exists for the same parent, return it instead of creating a duplicate
- a new deep research execution requires explicit retry if rerun is intended

Validation rules:

- parent job must exist;
- parent `job_type` must be `report`;
- parent must contain `report_markdown`.

### 12.8 Cancel Job

`POST /v1/jobs/{job_id}/cancel`

Response:

- `202 Accepted`

Behavior:

- if job is `queued`, API may switch directly to `canceled` and remove queue item if implementation supports it
- if job is `running`, API sets `cancel_requested` and worker must observe it cooperatively
- if job is terminal, API returns current job unchanged
- cancel should be naturally idempotent by state.

### 12.9 Retry Job

`POST /v1/jobs/{job_id}/retry`

Response:

- `202 Accepted`
- returns a new job with `retry_of_job_id = original_job_id`

Retry must create a new job. It must not reuse the same job row.

Validation rules:

- only terminal jobs may be retried;
- retry should reuse the same `source_id`;
- retry preserves `root_job_id`;
- retry preserves `parent_job_id` for child jobs.

### 12.10 Resolve Artifact Download

`GET /v1/artifacts/{artifact_id}`

Recommended v1 behavior:

- API returns artifact metadata plus a presigned MinIO URL
- client downloads using the returned URL

Clients must not construct MinIO URLs themselves and must not know bucket layout.

### 12.11 Job Events

`GET /v1/jobs/{job_id}/events`

Optional in first delivery, but recommended.

Useful for:

- UI event timeline
- debugging worker progress
- richer MCP introspection

Recommended response shape:

```json
{
  "items": []
}
```

### 12.12 Webhook Delivery Contract

When `delivery.strategy=webhook`, the API should POST lifecycle notifications to the configured URL.

Correct v1 webhook configuration shape:

```json
{
  "delivery": {
    "strategy": "webhook",
    "webhook": {
      "url": "http://host.docker.internal:8090/hooks/transcriber"
    }
  }
}
```

For local deployment in v1, `url` is the only mandatory webhook field.

Not included in v1:

- webhook signing secret
- custom headers
- custom retry policy per request
- per-request event filtering

Those may be added later if the local-only deployment model changes.

Recommended webhook envelope:

```json
{
  "event_id": "evt_uuid",
  "event_type": "job.completed",
  "job_id": "job_uuid",
  "root_job_id": "root_job_uuid",
  "job_type": "report",
  "version": 7,
  "emitted_at": "2026-04-19T12:00:00Z",
  "job_url": "/v1/jobs/job_uuid",
  "payload": {
    "status": "succeeded",
    "progress_stage": "completed",
    "progress_message": "Report ready"
  }
}
```

Rules:

- webhook is best-effort delivery, not the sole source of truth
- failed webhook deliveries should be retried by the API with bounded retry policy
- API still returns `job_id` and initial snapshot at submission time
- polling remains available as recovery/read access even for webhook-configured jobs
- treat any `2xx` as delivered
- retry on network error, timeout, `408`, `429`, and `5xx`
- do not retry other `4xx`
- recommended bounded backoff: `0s`, `10s`, `30s`, `2m`, `10m`
- receivers should deduplicate by `event_id` or `(job_id, version, event_type)`.

Recommended API error codes:

- `400`: `invalid_json`, `invalid_multipart`, `invalid_query`, `invalid_uuid`
- `404`: `job_not_found`, `artifact_not_found`
- `409`: `idempotency_conflict`, `invalid_job_state`, `retry_requires_terminal_job`
- `413`: `request_too_large`, `file_too_large`
- `415`: `unsupported_media_type`
- `422`: `validation_failed`, `webhook_url_required`, `invalid_webhook_url`, `unsupported_source_url`, `parent_job_not_succeeded`, `required_artifact_missing`, `display_name_not_allowed_for_multi_file_upload`
- `500`: `internal_error`
- `503`: `storage_unavailable`, `queue_unavailable`, `dependency_unavailable`

Recommended `job.error_code` set:

- `source_fetch_failed`
- `transcription_failed`
- `report_failed`
- `deep_research_failed`
- `artifact_upload_failed`
- `canceled`
- `internal_error`

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

Workers must not own arbitrary business state transitions through direct ad hoc SQL writes. They should use the API-owned contract or an internal control-plane library that enforces the same transition rules.

`execution_id` must be backed by authoritative control-plane persistence. It is not safe to infer worker ownership only from `jobs.started_at`, queue payloads, or in-memory route state.

Recommended status transitions:

```text
queued -> running
queued -> canceled
running -> succeeded
running -> failed
running -> cancel_requested
cancel_requested -> canceled
cancel_requested -> failed
```

Recommended worker lifecycle:

1. receive `asynq` task with `job_id`;
2. ask API or internal service layer to claim the job;
3. if claim fails because job is already terminal or owned, stop;
4. mark job `running`;
5. emit progress events;
6. execute the work;
7. upload artifacts to MinIO;
8. report artifact metadata;
9. finalize job as `succeeded`, `failed`, or `canceled`.

Delivery strategy note:

- workers do not choose between polling and webhook;
- workers only emit state changes;
- the API delivery layer decides whether the client receives updates by polling or webhook strategy.

Recommended internal control-plane endpoints:

- `POST /internal/v1/jobs/{job_id}/claim`
  - request:
    ```json
    {
      "worker_kind": "transcription",
      "task_type": "job:transcription.run"
    }
    ```
  - behavior:
    - atomically claims execution;
    - performs `queued -> running`;
    - sets `started_at`;
    - returns `execution_id`, lineage, inputs, params, and current version.
- `POST /internal/v1/jobs/{job_id}/progress`
  - request:
    ```json
    {
      "execution_id": "uuid",
      "progress_stage": "transcribing",
      "progress_message": "Processing audio chunks"
    }
    ```
- `POST /internal/v1/jobs/{job_id}/artifacts`
  - request:
    ```json
    {
      "execution_id": "uuid",
      "artifacts": [
        {
          "artifact_kind": "transcript_plain",
          "format": "txt",
          "filename": "transcript.txt",
          "mime_type": "text/plain",
          "object_key": "artifacts/job/transcript/plain/transcript.txt",
          "size_bytes": 123
        }
      ]
    }
    ```
  - behavior:
    - upsert artifact metadata by `(job_id, artifact_kind)`.
- `POST /internal/v1/jobs/{job_id}/finalize`
  - request:
    ```json
    {
      "execution_id": "uuid",
      "outcome": "succeeded",
      "progress_stage": "completed",
      "progress_message": "Transcript ready",
      "error_code": null,
      "error_message": null
    }
    ```
  - behavior:
    - validates required artifacts on success;
    - writes terminal state;
    - sets `finished_at`;
    - emits timeline, websocket, and webhook side effects.
- `GET /internal/v1/jobs/{job_id}/cancel-check?execution_id=uuid`
  - response:
    ```json
    {
      "cancel_requested": false,
      "status": "running",
      "cancel_requested_at": null
    }
    ```

Do not add heartbeat, lease renewal, worker registration, or pause/resume endpoints in v1.

### 14.1 Transcription Worker

Reused logic sources:

- current `transcribers.py`
- transcript rendering in `documents.py`
- source materialization logic from `service.py`

Outputs:

- `transcript_plain`
- `transcript_segmented_markdown`
- `transcript_docx`

Recommended progress stages:

- `queued`
- `materializing_source`
- `transcribing`
- `rendering_artifacts`
- `uploading_artifacts`
- `completed`
- `failed`
- `canceled`

### 14.2 Report Worker

Reused logic sources:

- `workers/report/src/transcriber_worker_report.py`
- `workers/common/src/transcriber_workers_common/documents.py`

Outputs:

- `report_markdown`
- `report_docx`

Recommended progress stages:

- `queued`
- `loading_transcript`
- `generating_report`
- `rendering_artifacts`
- `uploading_artifacts`
- `completed`
- `failed`
- `canceled`

### 14.3 Deep Research Worker

Reused logic sources:

- current `deep_research.py`

Outputs:

- `deep_research_markdown`
- optional execution log artifact

Recommended progress stages:

- `queued`
- `loading_report`
- `researching`
- `writing_output`
- `uploading_artifacts`
- `completed`
- `failed`
- `canceled`

## 15. Cancellation Semantics

Cancellation must be planned from day one.

### 15.1 API Rules

When cancel is requested:

- set `cancel_requested_at`;
- if running, set job status `cancel_requested`;
- emit event.
- if queued and still not claimed, API may finalize directly as `canceled`.

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
- retry must not copy artifacts or job events;
- child retry keeps the same `parent_job_id`;
- create-child operations must reuse an existing canonical child row rather than spawning sibling duplicates;
- if caller wants a new report or deep research run, the caller retries the existing child job instead of calling create-child again.
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
- optionally create jobs with `delivery.strategy=webhook` for advanced local integrations if surfaced later;
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
- upload multiple files in one request;
- submit YouTube URL;
- observe live state;
- view artifact list;
- download artifacts;
- create report job;
- create deep research job;
- cancel job;
- retry job.

Late-wave admin capabilities:

- delete job;
- delete job artifacts;
- re-resolve fresh download link for expired artifacts;
- manual rerun via explicit retry actions.

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

Runtime settings policy:

- Telegram bot does not expose advanced per-job runtime knobs in v1;
- it uses service defaults from the backend;
- bot chooses its delivery strategy explicitly per request;
- default bot strategy is `polling`.

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
- owns webhook delivery worker or background dispatcher

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

Recommended additional bind mounts or volumes:

- report/deep-research runtime temp space where needed;
- explicit log volume if service logs should be retained across restarts.

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
- `MAX_UPLOAD_SIZE_BYTES`
- `WEBHOOK_DELIVERY_ENABLED`
- `WEBHOOK_DELIVERY_MAX_ATTEMPTS`
- `WEBHOOK_DELIVERY_BACKOFF_SECONDS`

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

### 23.1 `src/media_analysis_platform/transcribers.py`

Move into:

- `workers/common/src/transcriber_workers_common/transcribers.py`
- `workers/transcription/src/...`

### 23.2 `src/media_analysis_platform/documents.py`

Move into:

- `workers/common/src/transcriber_workers_common/documents.py`

### 23.3 `workers/report/src/transcriber_worker_report.py`

Move into:

- `workers/report/src/...`

### 23.4 `src/media_analysis_platform/deep_research.py`

Move into:

- `workers/deep-research/src/...`

### 23.5 Former `src/media_analysis_platform/service.py` local shell

Was split into:

- execution-specific code into Python workers;
- job orchestration semantics into Go API service layer;
- metadata persistence logic replaced by PostgreSQL + MinIO + Redis integration.

### 23.6 `src/media_analysis_platform/bot.py`

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

#### Phase 0.4: Freeze newly accepted architectural decisions

Write a decision appendix from section `4.1 Frozen Design Decisions` into:

- queue choice `asynq`
- delivery strategy model `polling/webhook`
- MinIO presigned downloads
- multi-file upload
- no legacy compatibility track
- late-wave admin operations

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
- artifact resolve endpoint with presigned MinIO link
- optional idempotency
- delivery strategy and webhook fields
- multi-file upload request/response shape

#### Phase 1.3: Write WS schema draft

Define event envelope and event types.

#### Phase 1.3a: Write webhook schema draft

Define:

- webhook envelope
- retry semantics
- terminal and non-terminal lifecycle events
- delivery guarantees and failure handling

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
- Create: `apps/api/internal/storage/migrations/*.sql`
- Create: `apps/api/internal/ws/...`
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

Implement `asynq` enqueue functions by job type.

#### Phase 2.4: Add REST endpoints

Implement stubs first, then persistence-backed handlers.

#### Phase 2.5: Add WebSocket server

Implement WS connection, event broadcast, and reconnect-safe snapshot rule.

#### Phase 2.6: Add webhook delivery subsystem

Implement:

- stored webhook metadata on jobs
- background delivery loop
- bounded retry policy
- event emission rules per job type

### Wave 3: Transcription Worker Extraction

**Goal:** Move transcription execution out of the current monolithic bot into a standalone worker.

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

It must do this through `asynq` task handling and the API-owned control contract.

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

Move report execution and report DOCX generation into the report worker layer.

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

- multi-file upload
- YouTube URL submission

#### Phase 6.5: Add polling and WebSocket integration

Use polling as fallback and WS for fast updates.

#### Phase 6.6: Add actions

- report
- deep research
- cancel
- retry

#### Phase 6.7: Add job callback mode support

Support:

- default `polling`
- optional `webhook` form fields for advanced local use
- explicit explanation in UI that delivery strategy is either `polling` or `webhook`

Keep this feature secondary in the UI and do not overcomplicate primary flows.

#### Phase 6.8: Add late-wave admin operations

Implement on job details page:

- delete job
- delete artifacts
- refresh expired artifact download link
- explicit manual retry actions

### Wave 7: Telegram Bot Migration

**Goal:** Replace direct execution in Telegram bot with API calls.

**Files:**

- Create: `apps/telegram-bot/src/...`
- Create: `packages/sdk-py/...`
- Retire logic that had lived in `src/media_analysis_platform/service.py`

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

For completion waiting:

- bot submits with explicit delivery strategy;
- default to polling in bot flow.

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

#### Phase 9.4: Cut over old single-process entrypoint

There is no legacy compatibility track.

The required action is:

- remove or replace old single-process entrypoint once the new stack passes compose smoke verification and functional parity checks.

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
- artifact resolution with presigned MinIO link
- idempotency behavior
- webhook callback mode persistence and delivery

### 25.3 Worker Integration Tests

Required:

- transcription worker end-to-end against MinIO + Postgres + Redis
- report worker integration with report artifact output
- deep research worker integration with phase output checks

### 25.4 UI Tests

Required:

- upload file
- upload multiple files
- create URL job
- observe live transition via polling
- artifact download flow
- report and deep research triggers
- cancel and retry buttons
- admin delete and artifact refresh actions in late waves

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

### Risk 6: Big-bang cutover increases integration risk

Mitigation:

- freeze behavior baseline early;
- move UX only after API workers are already correct;
- require compose-wide smoke verification before replacing the old entrypoint;
- keep acceptance matrix explicit because there is no long-lived legacy track.

## 27. Definition of Done

The migration is complete only when all of the following are true:

- API server owns all business logic orchestration.
- Transcription, report, and deep research run only in workers.
- Telegram bot is a thin API client.
- MCP server is a thin API client.
- Web UI can create, observe, and manage jobs.
- PostgreSQL is authoritative job state storage.
- MinIO stores source files and artifacts.
- Redis-backed `asynq` is used for queueing and event fanout support.
- Polling and WebSocket are both implemented.
- Webhook callback mode is implemented for all job types.
- Retry creates new jobs.
- Cancellation works across all job types.
- Artifact downloads resolve to valid presigned MinIO links.
- Multi-file upload through the API works.
- Admin operations on the web details page exist in the planned late-wave scope.
- Docker Compose can bring up the full platform locally.
- Current core capabilities are preserved.

## 28. Immediate Next Task Recommendation

Implementation should start with Wave 0 and Wave 1 only.

Do not begin with UI or Telegram refactor first.

The recommended first coding order is:

1. create `packages/contracts`
2. bootstrap `apps/api`
3. add DB schema and `asynq` producer
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
- do not expose arbitrary runtime tuning knobs in Web UI or Telegram bot in v1;
- do not treat webhook delivery as stronger than polling;
- do not implement retry by mutating a finished job row.

The migration should always preserve the separation:

- Go API = control plane
- Python workers = execution plane
- UI/Bot/MCP = adapters
