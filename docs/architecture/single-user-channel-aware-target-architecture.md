# Single-User Channel-Aware Target Architecture

Status: source plan; implementation tracked by media-7f3.9, media-7f3.10, and media-7f3.11
Beads: media-7f3, media-w9y
Date: 2026-05-17

## Purpose

This document defines the target architecture for rebuilding `media-analysis-platform` as a product, not as a Telegram-only script.

The product assumption is deliberately simple:

- there is one human user;
- that user can interact through multiple channels: Telegram, Web, MCP, and future local or automation channels;
- the API owns product state;
- channels own presentation behavior;
- the system must survive process restarts without losing where results and task cards were rendered.

The goal is to minimize bad architectural decisions before implementation. This plan is intentionally broader than the current code state and should guide a staged rebuild of all apps, operations, schemas, types, workers, and tests.

## Data Reset Policy

The current local database is not a product source of truth and does not need to be preserved.

Implementation may delete, recreate, truncate, or fully rebuild the current database schema and data while moving to the target architecture. Do not spend implementation effort on preserving existing rows, backfilling historical local records, or proving no data loss for the current database.

What must be preserved is the product contract, not the current local data:

- target table semantics;
- clean schema initialization from an empty database;
- deterministic seed or fixture data for tests;
- restart-safe channel recovery after the new schema exists;
- artifact/object storage behavior for new runs created under the target model;
- clear reset/bootstrap instructions for local development.

If future production data preservation becomes a real requirement, it must be planned as a separate migration packet with its own acceptance criteria. It is explicitly out of scope for this target rebuild.

## FPF Framing

The architecture uses explicit bounded contexts. A word is valid only inside its context.

### Product Domain

Owns what the service is about:

- materials provided by the user;
- user-organized collections;
- immutable snapshots selected for processing;
- analysis runs;
- produced artifacts;
- diagnostics and retention.

Product domain does not know how Telegram, Web, or MCP renders anything.

### Execution Domain

Owns internal work:

- run steps;
- worker claiming;
- progress;
- cancellation observation;
- finalization.

Execution domain is behind `analysis_run`. Users see a run and its status; workers see steps and leases.

### Channel Domain

Owns external presentation and recovery:

- Telegram messages;
- Web panels when they need durable server-side recovery;
- MCP-visible operation surfaces if ever needed;
- external addresses;
- display metadata.

Channel domain may store intermediate state in the API, but it cannot mutate product lifecycle.

### Operations Domain

Owns idempotency, auditability, and safe retries:

- deduplicating channel commands;
- recording accepted mutation requests;
- making retries deterministic.

This is the small bit of operational glue that large systems usually keep separate from domain entities.

## Target Vocabulary

Use these names for the new target architecture.

| Concept | Target Name | Why |
| --- | --- | --- |
| user input | `media_asset` | Product object, not only a row item. |
| physical stored bytes | `stored_object` | Shared by media inputs and artifacts. |
| mutable grouping | `collection` | A current mutable set of media assets. |
| default mutable grouping | `inbox` | The default collection where new media lands. |
| execution input | `selection_snapshot` | Explicit immutable snapshot, not a mutable selection. |
| user-facing processing | `analysis_run` | Public run users can track/cancel/retry. |
| internal work unit | `analysis_run_step` | Step is clearer than job/task for run internals. |
| produced result | `artifact` | Durable result or evidence object. |
| warning/error/evidence | `diagnostic` | First-class operational and user-facing explanation. |
| external channel mapping | `channel_surface` | External surface in a channel, not a domain projection. |
| request idempotency | `operation_request` | Durable record of a mutation request. |

Avoid in target public contracts:

- `source` as a top-level product object;
- `media_item`;
- `selection` when the object is immutable;
- `job` as user-facing execution;
- adapter projection aliases;
- Telegram-specific table names.

## Target Table Set

The target starts with 18 tables. This is not minimal in raw count, but it is small for the responsibilities it separates.

```text
channel_accounts
operation_requests

stored_objects
media_assets

collections
collection_items

selection_snapshots
selection_snapshot_items

analysis_runs
analysis_run_steps
analysis_run_step_inputs
analysis_run_events

artifacts
artifact_subjects
diagnostics

channel_surfaces
channel_surface_subjects
channel_surface_events
```

Deferred until there is a real need:

```text
workspaces
workspace_members
media_asset_sources
artifact_versions
artifact_events
retention_policies
```

Do not add `owners` or `workspaces` for the current product stage. The product has one human user. Channel identity is enough.

## FPF Boundary Decisions

The target model is split into explicit bounded contexts. Same-looking words must not be treated as globally equivalent across these contexts.

### API Product Context

Owns durable product state and public product contracts:

- `media_asset`;
- `stored_object`;
- `collection` and `inbox`;
- `selection_snapshot`;
- `analysis_run`;
- `artifact`;
- `diagnostic`.

This context does not know Telegram card layouts, Web copy, or MCP client ergonomics.

### API Execution Context

Owns internal execution coordination behind `analysis_run`:

- `analysis_run_step`;
- `analysis_run_step_input`;
- worker claim/progress/finalize;
- prerequisite diagnostics.

This context may use worker-facing terms, but those terms do not become public product vocabulary.

### Channel Surface Context

Owns external presentation and recovery addresses:

- `channel_account`;
- `channel_surface`;
- `channel_surface_subject`;
- `channel_surface_event`.

Channel surfaces can point at product subjects, but they cannot drive product lifecycle.

### Web Human UI Context

Owns only human presentation. Normal Web UI copy uses simple product words such as `Материалы`, `Подборка`, `Результаты`, `Отчет`, `Ошибка`, `Скачать`, and `В работе`.

Normal Web UI must not make `media_asset`, `selection_snapshot`, `analysis_run_step`, `channel_surface`, raw ids, run manifests, or diagnostic internals load-bearing terms. Those names may appear only in explicit admin/debug views.

### MCP Tool Context

Owns technical tool contracts. MCP may expose target API vocabulary because its users are tools/agents, not casual human UI users.

### Removed-Surface Context

Pre-target implementation names are not an active product vocabulary. Current contracts, adapters, workers, Web, MCP, and GRACE packets must use the target terms in this document.

## Table Contracts

### channel_accounts

Purpose: store external channel identities for the single user.

Examples:

- Telegram private chat;
- Telegram bot user id;
- local Web profile;
- MCP client identity.

Recommended fields:

```sql
CREATE TABLE channel_accounts (
  id uuid PRIMARY KEY,
  channel text NOT NULL,
  external_account_ref text NOT NULL,
  display_name text,
  status text NOT NULL DEFAULT 'active',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz,
  disabled_at timestamptz,

  CONSTRAINT channel_accounts_status_chk CHECK (
    status IN ('active', 'disabled')
  )
);

CREATE UNIQUE INDEX channel_accounts_channel_ref_unique_idx
ON channel_accounts (channel, external_account_ref);
```

Responsibility:

- maps channel-specific identity to the single user;
- does not own product authorization;
- enables recovery and diagnostics by channel.

### operation_requests

Purpose: generic idempotency and audit record for accepted mutating operations.

Large systems usually treat idempotency as first-class for external channels because Telegram retries, user double taps, network timeouts, and worker restarts can duplicate requests.

Recommended fields:

```sql
CREATE TABLE operation_requests (
  id uuid PRIMARY KEY,
  channel_account_id uuid REFERENCES channel_accounts(id),
  operation_type text NOT NULL,
  idempotency_key text NOT NULL,
  request_hash text,
  status text NOT NULL,
  target_type text,
  target_id uuid,
  error_code text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,

  CONSTRAINT operation_requests_status_chk CHECK (
    status IN ('accepted', 'completed', 'failed', 'conflict')
  )
);

CREATE UNIQUE INDEX operation_requests_channel_idempotency_unique_idx
ON operation_requests (channel_account_id, operation_type, idempotency_key);
```

Responsibility:

- deduplicates creates and command callbacks;
- records what mutation a channel attempted;
- does not replace domain events.

### stored_objects

Purpose: shared metadata for object storage bytes, used by both input media and output artifacts.

Recommended fields:

```sql
CREATE TABLE stored_objects (
  id uuid PRIMARY KEY,
  bucket text NOT NULL,
  object_key text NOT NULL,
  content_type text,
  size_bytes bigint NOT NULL DEFAULT 0,
  checksum text,
  storage_status text NOT NULL DEFAULT 'available',
  retention_state text NOT NULL DEFAULT 'active',
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  deleted_at timestamptz,

  CONSTRAINT stored_objects_storage_status_chk CHECK (
    storage_status IN ('pending', 'available', 'missing', 'deleted')
  ),
  CONSTRAINT stored_objects_retention_state_chk CHECK (
    retention_state IN ('active', 'soft_deleted', 'expires_scheduled', 'expired', 'held')
  )
);

CREATE UNIQUE INDEX stored_objects_bucket_key_unique_idx
ON stored_objects (bucket, object_key);
```

Responsibility:

- knows physical storage facts;
- does not know whether bytes are input or output;
- allows unified retention and orphan cleanup.

### media_assets

Purpose: one user-provided input material.

Recommended fields:

```sql
CREATE TABLE media_assets (
  id uuid PRIMARY KEY,
  channel_account_id uuid REFERENCES channel_accounts(id),
  stored_object_id uuid REFERENCES stored_objects(id),
  origin_type text NOT NULL,
  origin_ref text,
  kind text NOT NULL,
  display_name text NOT NULL,
  status text NOT NULL DEFAULT 'available',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,

  CONSTRAINT media_assets_origin_type_chk CHECK (
    origin_type IN ('text', 'url', 'upload', 'telegram_file', 'generated')
  ),
  CONSTRAINT media_assets_status_chk CHECK (
    status IN ('ingesting', 'available', 'invalid', 'quarantined', 'deleted')
  )
);
```

Responsibility:

- product-level material;
- can exist without any collection item or run;
- stores origin summary, not full channel rendering state.

### collections

Purpose: mutable grouping of media assets.

Recommended fields:

```sql
CREATE TABLE collections (
  id uuid PRIMARY KEY,
  kind text NOT NULL,
  name text NOT NULL,
  status text NOT NULL DEFAULT 'active',
  version bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  archived_at timestamptz,
  deleted_at timestamptz,

  CONSTRAINT collections_kind_chk CHECK (kind IN ('inbox', 'user')),
  CONSTRAINT collections_status_chk CHECK (status IN ('active', 'archived', 'deleted')),
  CONSTRAINT collections_version_positive_chk CHECK (version >= 1)
);

CREATE UNIQUE INDEX collections_active_inbox_unique_idx
ON collections (kind)
WHERE kind = 'inbox' AND status = 'active';
```

Responsibility:

- mutable user organization;
- `inbox` is the current default place;
- not an execution snapshot.

### collection_items

Purpose: active and historical membership of assets inside a collection.

Recommended fields:

```sql
CREATE TABLE collection_items (
  id uuid PRIMARY KEY,
  collection_id uuid NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
  media_asset_id uuid NOT NULL REFERENCES media_assets(id),
  position int NOT NULL,
  added_via_channel_account_id uuid REFERENCES channel_accounts(id),
  added_at timestamptz NOT NULL DEFAULT now(),
  removed_at timestamptz,

  CONSTRAINT collection_items_position_nonnegative_chk CHECK (position >= 0)
);

CREATE UNIQUE INDEX collection_items_active_asset_unique_idx
ON collection_items (collection_id, media_asset_id)
WHERE removed_at IS NULL;

CREATE UNIQUE INDEX collection_items_active_position_unique_idx
ON collection_items (collection_id, position)
WHERE removed_at IS NULL;
```

Responsibility:

- preserves ordering;
- supports soft removal from a collection;
- does not delete the media asset.

### selection_snapshots

Purpose: immutable execution input captured from a collection.

Recommended fields:

```sql
CREATE TABLE selection_snapshots (
  id uuid PRIMARY KEY,
  source_collection_id uuid REFERENCES collections(id),
  status text NOT NULL DEFAULT 'sealed',
  option_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_via_channel_account_id uuid REFERENCES channel_accounts(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  sealed_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT selection_snapshots_status_chk CHECK (
    status IN ('sealed', 'invalidated')
  )
);
```

Responsibility:

- bridge from mutable collection to execution;
- never changes when collection changes later;
- captures options that influence processing.

### selection_snapshot_items

Purpose: immutable item rows inside a snapshot.

Recommended fields:

```sql
CREATE TABLE selection_snapshot_items (
  id uuid PRIMARY KEY,
  selection_snapshot_id uuid NOT NULL REFERENCES selection_snapshots(id) ON DELETE CASCADE,
  position int NOT NULL,
  media_asset_id uuid NOT NULL REFERENCES media_assets(id),
  kind text NOT NULL,
  display_name text NOT NULL,
  origin_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  storage_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  metadata_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  status_at_selection text NOT NULL,
  diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,

  CONSTRAINT selection_snapshot_items_position_unique UNIQUE (
    selection_snapshot_id,
    position
  )
);
```

Responsibility:

- freezes item facts needed by workers;
- keeps run reproducibility even after media metadata changes.

### analysis_runs

Purpose: user-facing processing run.

Recommended fields:

```sql
CREATE TABLE analysis_runs (
  id uuid PRIMARY KEY,
  selection_snapshot_id uuid NOT NULL REFERENCES selection_snapshots(id),
  run_type text NOT NULL,
  status text NOT NULL DEFAULT 'queued',
  version bigint NOT NULL DEFAULT 1,
  idempotency_key text,
  params jsonb NOT NULL DEFAULT '{}'::jsonb,
  delivery jsonb NOT NULL DEFAULT '{"strategy":"polling"}'::jsonb,
  evidence_gate_state text NOT NULL DEFAULT 'not_required',
  created_via_channel_account_id uuid REFERENCES channel_accounts(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  cancel_requested_at timestamptz,
  canceled_at timestamptz,
  expires_at timestamptz,

  CONSTRAINT analysis_runs_status_chk CHECK (
    status IN (
      'queued',
      'claiming',
      'running',
      'cancel_requested',
      'partially_succeeded',
      'succeeded',
      'failed',
      'canceled',
      'expired'
    )
  ),
  CONSTRAINT analysis_runs_version_positive_chk CHECK (version >= 1)
);
```

Responsibility:

- public execution lifecycle;
- cancellation, retry, progress, artifact lineage;
- not a worker lease table.

### analysis_run_steps

Purpose: internal worker-visible steps behind an analysis run.

Recommended fields:

```sql
CREATE TABLE analysis_run_steps (
  id uuid PRIMARY KEY,
  analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
  step_kind text NOT NULL,
  worker_kind text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  attempt_no int NOT NULL DEFAULT 1,
  lease_owner text,
  claimed_at timestamptz,
  heartbeat_at timestamptz,
  finalized_at timestamptz,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT analysis_run_steps_status_chk CHECK (
    status IN ('pending', 'queued', 'claimed', 'succeeded', 'partially_succeeded', 'failed', 'canceled')
  ),
  CONSTRAINT analysis_run_steps_attempt_positive_chk CHECK (attempt_no >= 1)
);
```

Responsibility:

- queue/worker coordination;
- retry attempts;
- cooperative cancellation observation.

### analysis_run_step_inputs

Purpose: explicit immutable inputs consumed by an execution step.

Recommended fields:

```sql
CREATE TABLE analysis_run_step_inputs (
  id uuid PRIMARY KEY,
  analysis_run_step_id uuid NOT NULL REFERENCES analysis_run_steps(id) ON DELETE CASCADE,
  input_kind text NOT NULL,
  selection_snapshot_item_id uuid REFERENCES selection_snapshot_items(id),
  artifact_id uuid REFERENCES artifacts(id),
  position int NOT NULL DEFAULT 0,
  required boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT analysis_run_step_inputs_kind_chk CHECK (
    input_kind IN ('selection_snapshot_item', 'transcript_artifact', 'text_corpus_artifact', 'metadata_artifact')
  ),
  CONSTRAINT analysis_run_step_inputs_subject_chk CHECK (
    (input_kind = 'selection_snapshot_item' AND selection_snapshot_item_id IS NOT NULL AND artifact_id IS NULL) OR
    (input_kind IN ('transcript_artifact', 'text_corpus_artifact', 'metadata_artifact') AND artifact_id IS NOT NULL)
  )
);
```

Responsibility:

- records exactly what a worker step may read;
- allows report/research steps to depend on transcript or text-corpus artifacts;
- prevents agent-runner from implicitly discovering raw speech media.

Migration note: because this table can reference `artifacts`, create it after `artifacts` or add the artifact foreign key in a later migration statement.

### analysis_run_events

Purpose: append-only user-visible and operational run history.

Recommended fields:

```sql
CREATE TABLE analysis_run_events (
  id uuid PRIMARY KEY,
  analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
  event_type text NOT NULL,
  version bigint NOT NULL,
  status text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT analysis_run_events_run_version_unique UNIQUE (
    analysis_run_id,
    version
  )
);
```

Responsibility:

- powers polling/websocket updates;
- explains transitions;
- separates history from current status.

### artifacts

Purpose: durable output or evidence produced by an analysis run.

Recommended fields:

```sql
CREATE TABLE artifacts (
  id uuid PRIMARY KEY,
  analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
  stored_object_id uuid REFERENCES stored_objects(id),
  kind text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  content_type text NOT NULL,
  checksum text,
  size_bytes bigint NOT NULL DEFAULT 0,
  visibility text NOT NULL DEFAULT 'private',
  preview jsonb NOT NULL DEFAULT '{"available":false}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  deleted_at timestamptz,

  CONSTRAINT artifacts_status_chk CHECK (
    status IN ('pending', 'available', 'failed', 'expired', 'deleted')
  ),
  CONSTRAINT artifacts_visibility_chk CHECK (
    visibility IN ('private', 'channel_deliverable')
  )
);
```

Responsibility:

- result metadata;
- object access through API;
- not tied to any Telegram message.

### artifact_subjects

Purpose: link artifacts to the concrete product or execution subjects they describe.

Recommended fields:

```sql
CREATE TABLE artifact_subjects (
  id uuid PRIMARY KEY,
  artifact_id uuid NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
  subject_type text NOT NULL,
  subject_id uuid NOT NULL,
  subject_role text NOT NULL DEFAULT 'primary',
  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT artifact_subjects_type_chk CHECK (
    subject_type IN (
      'analysis_run',
      'analysis_run_step',
      'selection_snapshot',
      'selection_snapshot_item',
      'media_asset',
      'diagnostic'
    )
  ),
  CONSTRAINT artifact_subjects_role_chk CHECK (
    subject_role IN ('primary', 'source', 'result', 'diagnostic', 'manifest_entry')
  )
);
```

Responsibility:

- tells API which transcript belongs to which selected material or step;
- lets agent-runner receive ordered transcript/text-corpus inputs without guessing;
- supports artifact detail, diagnostics, and lineage views across Telegram, Web, and MCP.

### diagnostics

Purpose: first-class warning, rejection, failure, and operator evidence.

Recommended fields:

```sql
CREATE TABLE diagnostics (
  id uuid PRIMARY KEY,
  subject_type text NOT NULL,
  subject_id uuid NOT NULL,
  severity text NOT NULL,
  code text NOT NULL,
  message text NOT NULL,
  context jsonb NOT NULL DEFAULT '{}'::jsonb,
  safe_channel_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  correlation_id text,
  remediation_hint text,
  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT diagnostics_severity_chk CHECK (
    severity IN ('info', 'warning', 'error')
  )
);
```

Responsibility:

- makes partial failures visible;
- supports operator debugging;
- safe by default: no bot token, no raw signed URLs.

### channel_surfaces

Purpose: external surface in a channel.

Recommended fields:

```sql
CREATE TABLE channel_surfaces (
  id uuid PRIMARY KEY,
  channel_account_id uuid REFERENCES channel_accounts(id),
  channel text NOT NULL,
  surface_type text NOT NULL,
  surface_key text NOT NULL,
  address jsonb NOT NULL DEFAULT '{}'::jsonb,
  address_fingerprint text,
  display_state jsonb NOT NULL DEFAULT '{}'::jsonb,
  lifecycle_status text NOT NULL DEFAULT 'active',
  version bigint NOT NULL DEFAULT 1,
  idempotency_key text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_rendered_at timestamptz,
  superseded_at timestamptz,
  deleted_at timestamptz,

  CONSTRAINT channel_surfaces_status_chk CHECK (
    lifecycle_status IN ('active', 'superseded', 'deleted', 'failed')
  )
);
```

Important indexes:

```sql
CREATE UNIQUE INDEX channel_surfaces_active_key_idx
ON channel_surfaces (
  channel_account_id,
  channel,
  surface_type,
  surface_key
)
WHERE lifecycle_status = 'active' AND deleted_at IS NULL;

CREATE UNIQUE INDEX channel_surfaces_active_address_idx
ON channel_surfaces (
  channel_account_id,
  channel,
  address_fingerprint
)
WHERE address_fingerprint IS NOT NULL
  AND lifecycle_status = 'active'
  AND deleted_at IS NULL;
```

Responsibility:

- restart-safe channel presentation mappings;
- no business lifecycle changes;
- no Telegram-specific domain fields.

### channel_surface_subjects

Purpose: link surfaces to the domain subjects they represent.

Recommended fields:

```sql
CREATE TABLE channel_surface_subjects (
  surface_id uuid NOT NULL REFERENCES channel_surfaces(id) ON DELETE CASCADE,
  subject_type text NOT NULL,
  subject_id uuid NOT NULL,
  subject_role text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (surface_id, subject_role, subject_type, subject_id),
  CONSTRAINT channel_surface_subjects_role_chk CHECK (
    subject_role IN ('primary', 'context', 'result', 'diagnostic', 'navigation_target')
  )
);

CREATE UNIQUE INDEX channel_surface_one_primary_subject_idx
ON channel_surface_subjects (surface_id)
WHERE subject_role = 'primary';
```

Responsibility:

- supports one surface representing run plus selection, or artifact plus run;
- enables subject-scoped lookup;
- prevents channel tables from duplicating domain columns.

### channel_surface_events

Purpose: append-only operational history for surfaces.

Recommended fields:

```sql
CREATE TABLE channel_surface_events (
  id uuid PRIMARY KEY,
  surface_id uuid REFERENCES channel_surfaces(id) ON DELETE SET NULL,
  event_type text NOT NULL,
  reason text,
  previous_version bigint,
  next_version bigint,
  actor_type text NOT NULL,
  actor_id text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

Responsibility:

- explains duplicate prevention;
- supports recovery investigations;
- feeds diagnostics if channel delivery fails repeatedly.

## API Operation Groups

### Channel Accounts

Internal or admin-only:

```text
GET    /internal/v1/channel-accounts
PUT    /internal/v1/channel-accounts
PATCH  /internal/v1/channel-accounts/{channel_account_id}
```

Operations:

- resolve Telegram chat/user identity;
- mark channel account disabled;
- update last seen.

### Media Assets

Public app-facing:

```text
POST   /v1/media-assets
POST   /v1/media-assets/upload
GET    /v1/media-assets
GET    /v1/media-assets/{media_asset_id}
DELETE /v1/media-assets/{media_asset_id}
```

Operations:

- accept text, URL, uploaded file, Telegram file reference;
- write `stored_objects` when bytes are present;
- create `media_assets`;
- append accepted assets to inbox collection;
- expose invalid/quarantined diagnostics instead of silent drops.

### Collections

```text
GET    /v1/collections/inbox
POST   /v1/collections
GET    /v1/collections
GET    /v1/collections/{collection_id}
PATCH  /v1/collections/{collection_id}
POST   /v1/collections/{collection_id}/items
DELETE /v1/collections/{collection_id}/items/{media_asset_id}
```

Operations:

- current materials view;
- append/reorder/remove/clear items;
- optimistic version checks;
- never mutate selection snapshots.

### Selection Snapshots

```text
POST /v1/selection-snapshots
GET  /v1/selection-snapshots/{selection_snapshot_id}
```

Operations:

- freeze collection state;
- validate media availability;
- store option snapshot;
- record per-item diagnostics.

### Analysis Runs

```text
POST /v1/analysis-runs
GET  /v1/analysis-runs
GET  /v1/analysis-runs/{analysis_run_id}
POST /v1/analysis-runs/{analysis_run_id}/cancel
POST /v1/analysis-runs/{analysis_run_id}/retry
GET  /v1/analysis-runs/{analysis_run_id}/events
```

Operations:

- create run from selection snapshot;
- schedule initial steps and prerequisite steps;
- for `report` or `deep_research`, require ready transcript/text-corpus artifacts or create transcription prerequisite steps before agent-runner steps;
- when prerequisites cannot be planned or satisfied, record diagnostics with stable codes `analysis_prerequisite_missing`, `analysis_prerequisite_failed`, or `analysis_prerequisite_unavailable`;
- expose status and progress;
- cooperative cancel;
- retry from immutable snapshot.

### Artifacts

```text
GET  /v1/artifacts
GET  /v1/artifacts/{artifact_id}
POST /v1/artifacts/{artifact_id}/refresh
GET  /internal/v1/artifacts/{artifact_id}/download-access
```

Operations:

- list run outputs;
- resolve safe access;
- refresh temporary links;
- expose artifact subjects and lineage;
- keep output independent of channel delivery.

### Diagnostics

```text
GET /v1/diagnostics
GET /v1/diagnostics/{diagnostic_id}
```

Operations:

- filter by subject, severity, code, correlation;
- expose safe remediation text;
- support troubleshooting from Telegram and Web.

### Channel Surfaces

Internal channel runtime API:

```text
PUT   /internal/v1/channel-surfaces
GET   /internal/v1/channel-surfaces
GET   /internal/v1/channel-surfaces/active
POST  /internal/v1/channel-surfaces/{surface_id}/supersede
PATCH /internal/v1/channel-surfaces/{surface_id}/display-state
GET   /internal/v1/channel-surfaces/{surface_id}/events
```

Operations:

- upsert surface by `channel_account_id + channel + surface_type + surface_key`;
- replace display state by `expected_version`;
- list active surfaces on channel restart;
- find surfaces by represented subject;
- supersede stale surfaces when messages are not editable;
- append channel surface events.

### Worker Internal API

```text
GET  /internal/v1/analysis-runs/queue
POST /internal/v1/analysis-runs/{analysis_run_id}/steps/claim
GET  /internal/v1/analysis-runs/{analysis_run_id}/steps/cancel-check
POST /internal/v1/analysis-runs/{analysis_run_id}/steps/progress
POST /internal/v1/analysis-runs/{analysis_run_id}/artifacts
POST /internal/v1/analysis-runs/{analysis_run_id}/diagnostics
POST /internal/v1/analysis-runs/{analysis_run_id}/steps/finalize
```

Operations:

- workers only consume sealed `selection_snapshots`;
- worker claim responses include declared `analysis_run_step_inputs`;
- workers never read mutable collections as execution input;
- transcription workers turn selected media snapshots into transcript artifacts;
- report/research workers consume transcript or prepared text-corpus artifacts, not raw audio/video media, whenever a transcription prerequisite exists;
- agent-runner steps fail through prerequisite diagnostics rather than attempting speech-to-text or raw media discovery;
- finalization respects `cancel_requested`.

### Removed Public Surfaces

The target architecture does not keep public compatibility routes or execution wrappers. Current public clients use these surfaces directly:

```text
/v1/media-assets
/v1/selection-snapshots
/v1/analysis-runs
/v1/artifacts
/v1/diagnostics
```

Rules:

- no adapter, worker, Web, or MCP code should depend on removed public surfaces;
- contracts must expose the target DTOs directly;
- tests must prove removed vocabulary does not leak into target OpenAPI, Web copy, Telegram copy, MCP tool names, or worker DTO names.

## DTO And Type Naming

### Go API Storage Types

Target names:

```text
ChannelAccountRecord
OperationRequestRecord
StoredObjectRecord
MediaAssetRecord
CollectionRecord
CollectionItemRecord
SelectionSnapshotRecord
SelectionSnapshotItemRecord
AnalysisRunRecord
AnalysisRunStepRecord
AnalysisRunStepInputRecord
AnalysisRunEventRecord
ArtifactRecord
ArtifactSubjectRecord
DiagnosticRecord
ChannelSurfaceRecord
ChannelSurfaceSubjectRecord
ChannelSurfaceEventRecord
```

Request names:

```text
CreateMediaAssetRequest
UpdateCollectionItemsRequest
CreateSelectionSnapshotRequest
CreateAnalysisRunRequest
CancelAnalysisRunRequest
RecordAnalysisRunStepProgressRequest
FinalizeAnalysisRunStepRequest
UpsertChannelSurfaceRequest
ReplaceChannelSurfaceDisplayStateRequest
SupersedeChannelSurfaceRequest
```

### HTTP JSON Names

Use snake_case:

```text
media_asset_id
selection_snapshot_id
analysis_run_id
analysis_run_step_id
artifact_id
diagnostic_id
channel_surface_id
surface_key
display_state
address_fingerprint
```

Avoid:

```text
pre-target media identifiers
mutable selection aliases
external job aliases
transport-specific message identifiers
```

No migration compatibility layer remains in the active runtime.

## App Responsibilities

### apps/api

Owns:

- all tables;
- migrations;
- public contracts;
- internal worker contracts;
- internal channel surface contracts;
- idempotency and optimistic locking;
- diagnostics and retention;
- websocket/event stream.

Does not own:

- Telegram text rendering;
- Web component state;
- worker speech recognition logic;
- model/provider-specific quality decisions.

### apps/telegram-bot

Owns:

- Telegram update parsing;
- media upload/download handoff;
- Russian user-facing copy;
- inline keyboards;
- callback encoding;
- channel surface creation/update/supersede;
- restart recovery from active channel surfaces.

Does not own:

- product state;
- run lifecycle;
- artifact ownership;
- durable media selection state outside API.

### apps/web

Owns:

- simple human-facing management UI;
- browsing materials in plain user terms;
- editing named groups and current selections through API calls;
- launching analysis from the current selection;
- reviewing results, progress, errors, and downloads in user language.

Web is intentionally "dumb": it renders understandable state and actions over public API contracts, but it does not own business semantics, durable workflow state, worker orchestration, or product vocabulary decisions. Internal terms such as `media_asset`, `selection_snapshot`, `analysis_run_step`, `channel_surface`, opaque ids, and diagnostic internals are not load-bearing UI copy outside explicit admin/debug views.

Does not need channel surfaces for MVP unless a Web-specific server-side surface recovery feature is introduced.

### apps/mcp-server

Owns:

- tool schema;
- tool-to-API mapping;
- deterministic channel account identity;
- returning structured results, diagnostics, and artifact references.

Does not own:

- business state;
- worker execution;
- local durable storage.

### workers/transcription

Owns:

- claiming transcription steps;
- materializing input objects;
- running transcription provider;
- recording transcript artifacts;
- recording diagnostics;
- observing cancellation.

### workers/agent-runner

Owns:

- report/research steps;
- reading transcript artifacts or prepared text-corpus artifacts produced for a sealed snapshot;
- producing report artifacts;
- recording diagnostics and partial success.

Does not own transcription. For audio, video, voice, or other media that requires speech-to-text, the API orchestration must provide a completed transcript/text input first or schedule the transcription prerequisite before the agent-runner step. If the required transcript artifact is missing, the worker records or receives a prerequisite diagnostic instead of trying to process raw media itself.

### workers/common

Owns:

- shared API client;
- object store helpers;
- provider adapters;
- document rendering;
- testable provider abstractions.

## User Flows

### Add Media From Telegram

1. Telegram receives voice/file/text/link.
2. Telegram resolves or creates `channel_account`.
3. Telegram sends media to API.
4. API creates `stored_object` if bytes exist.
5. API creates `media_asset`.
6. API appends it to inbox `collection`.
7. Telegram updates or creates `channel_surface` with `surface_type=current_materials_panel`.
8. User sees `Подборка`.

### Start Processing

1. User taps `Обработать`.
2. Telegram calls create selection snapshot from inbox collection.
3. API creates `selection_snapshot` and items.
4. API creates `analysis_run`.
5. API creates initial `analysis_run_step`.
6. Telegram creates task card and upserts `channel_surface` with `surface_type=analysis_task_surface`.
7. Telegram clears current collection through API.
8. Telegram updates current materials panel.

### Worker Completes Run

1. Worker claims run step.
2. Worker reads `selection_snapshot_items`.
3. Worker records progress events.
4. Worker writes transcript to object storage and creates `stored_object`.
5. Worker records `artifact`.
6. Worker finalizes step and run.
7. Telegram watcher sees run completion.
8. Telegram sends document and upserts `channel_surface` with `surface_type=result_artifact_surface`.

### Report Or Research From Transcript

1. API creates or finds a sealed `selection_snapshot`.
2. API ensures required transcript/text-corpus artifacts exist for audio, video, and voice materials.
3. If text prerequisites are missing but schedulable, API creates transcription prerequisite steps and declares downstream artifact inputs.
4. If text prerequisites are not schedulable or fail, API records `analysis_prerequisite_missing`, `analysis_prerequisite_failed`, or `analysis_prerequisite_unavailable` diagnostics on the run.
5. Agent-runner claims the report or research step only after declared artifact inputs are ready.
6. Agent-runner reads only ready transcript/text-corpus artifact inputs and safe metadata.
7. Agent-runner records progress events, diagnostics, and report/research artifacts.
8. API exposes the produced artifacts to Telegram, Web, and MCP through the same run detail contract.

### Restart Recovery

1. Telegram bot starts.
2. It resolves channel account.
3. It lists active `channel_surfaces`.
4. It resumes watchers for `analysis_task_surface` records whose primary subject is an active `analysis_run`.
5. It restores or recreates the current materials panel.
6. If an external message cannot be edited, it supersedes the old surface and creates a new one.

## Implementation Stages

### Stage 0: Architecture Freeze

Goal: make docs, Beads, and GRACE agree on the target model.

Files:

- `docs/architecture/single-user-channel-aware-target-architecture.md`;
- `docs/requirements.xml`;
- `docs/development-plan.xml`;
- `docs/knowledge-graph.xml`;
- `docs/operational-packets.xml`;
- `docs/verification-plan.xml`.

Verification:

- XML validates;
- stale vocabulary search passes;
- Beads graph exists.

### Stage 1: Contract And Type Rename

Goal: expose target DTO names directly and remove pre-target contract surfaces.

Work:

- define target schemas;
- remove public compatibility routes and wrappers;
- use target route names without a feature flag or parallel namespace;
- update generated client types if any.

Verification:

- contract tests for new DTOs;
- stale public vocabulary tests;
- adapters pass against target routes only.

### Stage 2: Storage Reset And Rebuild

Goal: rebuild the database toward target tables without preserving current local data.

Work:

- add `channel_accounts`;
- add `operation_requests`;
- add `stored_objects`;
- add `media_assets`;
- create immutable `selection_snapshots`;
- create worker-claimable `analysis_run_steps`;
- add `analysis_run_step_inputs`;
- add `artifact_subjects`;
- add `channel_surfaces`, `channel_surface_subjects`, `channel_surface_events`;
- provide reset/bootstrap instructions for an empty local database.

Verification:

- schema reset/recreate proof;
- storage channel-account/single-user invariants;
- snapshot immutability tests;
- channel surface uniqueness tests;
- deterministic seed/fixture proof for tests.

### Stage 3: API Orchestration

Goal: make API the only state owner.

Work:

- implement media asset service;
- implement collection service;
- implement snapshot service;
- implement run service;
- implement prerequisite planning for run steps;
- implement step input resolution from selection snapshot items and artifact subjects;
- implement prerequisite diagnostic codes: `analysis_prerequisite_missing`, `analysis_prerequisite_failed`, and `analysis_prerequisite_unavailable`;
- implement artifact service;
- implement diagnostics service;
- implement channel surface service;
- implement operation request idempotency.

Verification:

- every mutation has success, validation failure, conflict, idempotent replay, and persistence tests;
- every read has empty and pagination tests;
- internal worker routes stay isolated from public product routes.

### Stage 4: Telegram Migration

Goal: Telegram becomes a channel runtime over the new API.

Work:

- replace local message tracking with channel surfaces;
- current materials panel is always a `current_materials_panel` surface;
- task cards are `analysis_task_surface`;
- result files are `result_artifact_surface`;
- callback payloads use API ids and versions;
- restart recovery uses API surfaces.

Verification:

- no dead-end active run state;
- new current materials can launch while old run is active;
- restart resumes active task card;
- completion after restart does not duplicate result file;
- Russian copy stays user-facing and hides technical names.

### Stage 5: Workers Migration

Goal: workers operate on `analysis_run_steps` and `selection_snapshots`.

Work:

- claim steps, not tasks;
- read snapshots, not collections;
- read declared step inputs, not ad hoc raw media discovery;
- make transcription the only worker path that converts raw speech media into transcript artifacts;
- make agent-runner consume an ordered manifest of ready transcript/text-corpus artifacts for report and research work;
- publish artifacts through API;
- record diagnostics through API;
- observe cancellation.

Verification:

- sealed snapshot consumption;
- declared step input consumption;
- missing prerequisite diagnostics;
- cancellation race tests;
- partial success diagnostics;
- artifact publication;
- provider failure mapping.

### Stage 6: Web And MCP Migration

Goal: Web and MCP use target API operations while Web keeps user-facing language simple and non-technical.

Work:

- Web uses API-owned media assets, collections, snapshots, runs, artifacts, and diagnostics without exposing those names as normal UI labels;
- Web copy prefers clear Russian product words such as `Материалы`, `Подборка`, `Результаты`, `Отчет`, `Ошибка`, and `Скачать`;
- Web normal UI must avoid `Run builder`, `Artifacts`, `Diagnostics`, `Run manifest`, `Selection snapshot`, `Analysis run`, `Media item`, raw ids, and diagnostic internals unless inside explicit admin/debug surfaces;
- MCP tools expose target vocabulary and may stay technical because MCP is a tool contract context;
- old names removed from user-visible contract.

Verification:

- route tests;
- tool schema tests;
- UI tests for empty states and run detail;
- no stale or internal vocabulary in visible Web text outside admin/debug surfaces.

### Stage 7: Runtime And Coverage Closure

Goal: prove the product end to end.

Work:

- compose smoke;
- Telegram runtime proof;
- worker runtime proof;
- artifact download proof;
- restart recovery proof;
- coverage report.

Verification:

- every table has storage tests;
- every API operation has handler and service tests;
- every app has contract tests against target DTOs;
- every user flow has at least one integration or runtime proof;
- all known failure states expose diagnostics.

## Verification Matrix

| Area | Required Proof |
| --- | --- |
| Database | Schema reset/recreate proof, constraints, indexes, deterministic seed fixtures. |
| Storage | CRUD, channel-account/single-user scope, idempotency, version conflicts. |
| API | Route success, validation errors, conflict errors, pagination, empty arrays. |
| Telegram | User flows, callback parsing, restart recovery, surface supersede, result delivery. |
| Web | Main flows, empty states, diagnostics, artifacts, plain human copy without internal technical terms. |
| MCP | Tool schema, error mapping, idempotency. |
| Workers | Claim, progress, cancel, artifact, diagnostic, finalize, agent-runner transcript/text-corpus input contract. |
| Diagnostics | Every rejection and partial failure has code/message/remediation. |
| Retention | Media, artifacts, surfaces, events, and orphan cleanup. |
| Vocabulary | Public docs and UI avoid deleted terms. |

100% coverage means every operation and invariant above has explicit tests. Line coverage is useful but not sufficient by itself.

## Beads Execution Decomposition

The executable Beads graph follows the `plan-to-beads` three-stage shape. The source plan remains this document; Beads are execution handles, not a second source of truth.

### Implementation Epic

`media-7f3.9`: Implementation Epic: single-user channel-aware target architecture.

Tasks:

1. `media-7f3.9.1`: Implement target contracts and removed-surface policy.
2. `media-7f3.9.2`: Implement target storage reset/rebuild and repositories.
3. `media-7f3.9.3`: Implement target API domain services and operations.
4. `media-7f3.9.4`: Implement channel surfaces and restart recovery.
5. `media-7f3.9.5`: Migrate Telegram bot to target materials and run-card flow.
6. `media-7f3.9.6`: Migrate workers to analysis_run_steps over selection_snapshots.
7. `media-7f3.9.7`: Migrate Web and MCP to target vocabulary and API operations.
8. `media-7f3.9.8`: Implementation cleanup and GRACE artifact refresh.

### Full Test Coverage Epic

`media-7f3.10`: Full Test Coverage Epic: target architecture proof.

Coverage matrix and deterministic fixture source: `docs/architecture/target-coverage-matrix.md`.

Tasks:

1. `media-7f3.10.1`: Build target coverage matrix and deterministic test environment.
2. `media-7f3.10.2`: Add storage and API coverage for target contracts.
3. `media-7f3.10.3`: Add Telegram, worker, Web, and MCP E2E coverage.
4. `media-7f3.10.4`: Wire coverage inventory and no-legacy regression gates.

### Pre-MR QA Epic

`media-7f3.11`: Pre-MR QA Epic: target architecture readiness.

Tasks:

1. `media-7f3.11.1`: QA traceability audit against source plan and Beads graph.
2. `media-7f3.11.2`: QA backend, storage, security, and worker review.
3. `media-7f3.11.3`: QA channel UX, Web, MCP, and runtime behavior.
4. `media-7f3.11.4`: Prepare final MR readiness packet for target rebuild.

Superseded flat tasks:

- `media-7f3.1` -> `media-7f3.9.1`
- `media-7f3.2` -> `media-7f3.9.2`
- `media-7f3.3` -> `media-7f3.9.3`
- `media-7f3.4` -> `media-7f3.9.4`
- `media-7f3.5` -> `media-7f3.9.5`
- `media-7f3.6` -> `media-7f3.9.6`
- `media-7f3.7` -> `media-7f3.9.7`
- `media-7f3.8` -> `media-7f3.10`

## Non-Goals

- No multi-user team model yet.
- No workspaces yet.
- No public Telegram-specific API.
- No local durable Telegram state outside API.
- No user-facing job/task aliases, adapter projection aliases, or mutable-selection wording when meaning is snapshot.
- No shortcut that lets workers read mutable collections.

## Final Architecture Rule

If a fact answers “what the user is analyzing,” it belongs to product domain tables.

If a fact answers “how the work is being executed,” it belongs to execution tables.

If a fact answers “where this is displayed in Telegram/Web/MCP,” it belongs to channel surface tables.

If a fact answers “why this retry or duplicate did not create a second object,” it belongs to operation requests or events.
