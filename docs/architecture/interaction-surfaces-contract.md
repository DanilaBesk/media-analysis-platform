# Interaction Surfaces Contract

Status: planned
Beads: media-cgr, media-fal
Date: 2026-05-16

## Purpose

This document defines a generic API-owned Interaction Surfaces boundary for external channel surfaces that represent API-owned domain subjects.

The immediate driver is the Telegram card model in `docs/architecture/telegram-selection-transcription-cards.md`: Telegram needs durable mappings between current materials cards, transcription task cards, result file messages, and API-owned subjects. The API should provide durability and owner-scoped consistency without becoming aware of Telegram-specific UI concepts.

The design goal is:

- keep the domain API abstract and consumer-independent;
- avoid Telegram-specific fields on domain tables;
- support restart recovery for channel runtimes;
- keep future Slack, email, Web, and mobile surfaces possible;
- separate durable product state from external channel display state.

## Simple Concept

An interaction surface is a durable record saying:

> Channel X rendered one or more API-owned subjects as external surface Z.

The API stores the mapping. The channel owns the meaning of the external reference and the visual rendering.

Example:

```json
{
  "channel": "telegram",
  "surface_type": "analysis_task_surface",
  "surface_key": "analysis_run:run-123",
  "address": {
    "chat_id": "10",
    "thread_id": null,
    "message_id": "9010"
  },
  "subjects": [
    {
      "subject_type": "analysis_run",
      "subject_id": "run-123",
      "subject_role": "primary"
    }
  ]
}
```

The API does not need to know that this is a Telegram message. It only knows that an external interaction surface exists for owner-scoped subjects.

## Naming Rationale

The boundary is intentionally named `Interaction Surfaces`, not after an implementation-level rendering pattern.

Rendering patterns describe how a runtime derives a view, but the API record is not a domain view of `analysis_run` or `artifact`. It is a durable fact that a domain subject is represented at an external address. Runtime names also describe the worker that performs work, not the stable domain concept stored in the API.

Normative contract names therefore use:

```text
interaction_surface  -- the stored record
channel              -- external communication environment, such as telegram or slack
surface_type         -- generic role of the external surface
address              -- opaque external location
display_state        -- opaque channel-owned render metadata
lifecycle_status     -- storage lifecycle of this surface record
```

## Context Map

This contract sits between three bounded contexts and must not collapse their language.

### Product Domain

Owns canonical media state:

- `media_item`;
- `collection` and `inbox`;
- immutable `selection`;
- `analysis_run`;
- `artifact`;
- `diagnostic`;
- retention and owner scope.

Product domain state is valid even when no external interaction surface exists.

### Interaction Surfaces

Owns only the durable representation link:

```text
domain subject set -> channel surface address
```

It can validate owner scope and lifecycle, but it must not interpret the rendered text, buttons, Telegram callback payloads, or product lifecycle decisions.

### Channel Runtimes

Own rendering and transport behavior:

- Telegram message edits and sends;
- Slack or email provider calls;
- button layout and user-facing copy;
- recovery behavior when an external address is stale or unavailable.

Channel runtimes use this API to recover and coordinate external surfaces, but they do not move durable product state out of the domain API.

## Boundary Rules

### API Owns

- storing interaction surface records;
- owner-scope enforcement;
- idempotent upsert semantics;
- lookup by channel, owner, subject, and surface type;
- lifecycle flags such as active, superseded, deleted;
- timestamps and optimistic versioning;
- retention-compatible cleanup rules.

### Channel Runtime Owns

- rendering text, buttons, files, and UI details;
- interpreting `address`;
- deciding whether to edit, resend, or supersede an external surface;
- channel-specific display metadata stored in `display_state`;
- recovery behavior when an external message no longer exists.

### Domain API Owns

- media items;
- collections and inbox;
- immutable selections;
- analysis runs;
- artifacts;
- diagnostics;
- retention state.

The domain API must not depend on interaction surfaces.

## Non-Goals

- Do not add `telegram_message_id`, `telegram_chat_id`, or similar fields to `analysis_runs`, `collections`, `selections`, or `artifacts`.
- Do not create Telegram-specific API tables such as `telegram_cards`.
- Do not expose interaction surfaces as a public product feature in Web, MCP, or external contracts.
- Do not let interaction surface display state change domain lifecycle.
- Do not make the API interpret button layouts, card texts, or consumer UI states.

## Module Boundary

Recommended module:

```text
apps/api/internal/interaction_surfaces
```

or, if the current API organization prefers existing package grouping:

```text
apps/api/internal/storage/interaction_surfaces.go
apps/api/internal/api/interaction_surface_handlers.go
```

Conceptually this is a separate boundary:

```text
Domain API
  media_item / collection / selection / analysis_run / artifact / diagnostics

Interaction Surfaces API
  durable external rendering mappings
```

They can share one database and one API service, but their contracts must remain separate.

## Data Model

The API provides durable intermediate storage for channel runtimes through three tables:

1. `interaction_surfaces` stores the external surface record.
2. `interaction_surface_subjects` links a surface to API-owned domain subjects.
3. `interaction_surface_events` keeps an append-only operational history for recovery and diagnostics.

Do not add Telegram-specific tables or fields to domain entities. The surface layer is generic storage for consumers, but the API still owns validation, lifecycle, versioning, and owner-scope consistency.

### interaction_surfaces

One row represents one external surface: a Telegram message, Slack message, email, Web panel, or future channel surface.

```sql
CREATE TABLE interaction_surfaces (
  id uuid PRIMARY KEY,
  owner_type TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  tenant_id TEXT,

  channel TEXT NOT NULL,
  surface_type TEXT NOT NULL,
  surface_key TEXT NOT NULL,

  address JSONB NOT NULL DEFAULT '{}'::jsonb,
  address_fingerprint TEXT,
  display_state JSONB NOT NULL DEFAULT '{}'::jsonb,

  lifecycle_status TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  idempotency_key TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_rendered_at TIMESTAMPTZ,
  superseded_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ,

  CONSTRAINT interaction_surfaces_status_chk CHECK (
    lifecycle_status IN ('active', 'superseded', 'deleted', 'failed')
  ),
  CONSTRAINT interaction_surfaces_version_positive_chk CHECK (version >= 1)
);
```

Recommended lifecycle_status values:

```text
active
superseded
deleted
failed
```

Recommended uniqueness:

```sql
CREATE UNIQUE INDEX interaction_surfaces_active_key_idx
ON interaction_surfaces (
  owner_type,
  owner_id,
  COALESCE(tenant_id, ''),
  channel,
  surface_type,
  surface_key
)
WHERE lifecycle_status = 'active' AND deleted_at IS NULL;
```

Recommended duplicate-address protection:

```sql
CREATE UNIQUE INDEX interaction_surfaces_active_address_idx
ON interaction_surfaces (
  owner_type,
  owner_id,
  COALESCE(tenant_id, ''),
  channel,
  address_fingerprint
)
WHERE address_fingerprint IS NOT NULL
  AND lifecycle_status = 'active'
  AND deleted_at IS NULL;
```

Recommended active lookup index:

```sql
CREATE INDEX interaction_surfaces_owner_channel_idx
ON interaction_surfaces (
  owner_type,
  owner_id,
  tenant_id,
  channel,
  lifecycle_status,
  updated_at DESC
);
```

### interaction_surface_subjects

This table links an external surface to the domain subjects it represents. It is separate because one visible surface can involve multiple subjects. For example, a task card primarily represents an `analysis_run`, but it also refers to a `selection`; a result file message primarily represents an `artifact`, but it also belongs to an `analysis_run`.

```sql
CREATE TABLE interaction_surface_subjects (
  surface_id uuid NOT NULL REFERENCES interaction_surfaces(id) ON DELETE CASCADE,

  subject_type TEXT NOT NULL,
  subject_id uuid NOT NULL,
  subject_role TEXT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (surface_id, subject_role, subject_type, subject_id),
  CONSTRAINT interaction_surface_subjects_type_chk CHECK (
    subject_type IN ('collection', 'selection', 'analysis_run', 'artifact', 'diagnostic')
  ),
  CONSTRAINT interaction_surface_subjects_role_chk CHECK (
    subject_role IN ('primary', 'context', 'result', 'diagnostic', 'navigation_target')
  )
);
```

Recommended lookup index:

```sql
CREATE INDEX interaction_surface_subject_lookup_idx
ON interaction_surface_subjects (
  subject_type,
  subject_id,
  subject_role
);
```

Recommended one-primary-subject rule:

```sql
CREATE UNIQUE INDEX interaction_surface_one_primary_subject_idx
ON interaction_surface_subjects (surface_id)
WHERE subject_role = 'primary';
```

### interaction_surface_events

This table is append-only operational history. It is not required to render the UI, but it is valuable for restart recovery, debugging, duplicate-send investigations, and future observability.

```sql
CREATE TABLE interaction_surface_events (
  id uuid PRIMARY KEY,
  surface_id uuid REFERENCES interaction_surfaces(id) ON DELETE SET NULL,

  event_type TEXT NOT NULL,
  reason TEXT,

  previous_version BIGINT,
  next_version BIGINT,

  actor_type TEXT NOT NULL,
  actor_id TEXT,

  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT interaction_surface_events_type_chk CHECK (
    event_type IN (
      'created',
      'upserted',
      'address_changed',
      'display_state_replaced',
      'superseded',
      'deleted',
      'failed',
      'recovered'
    )
  )
);
```

Recommended event index:

```sql
CREATE INDEX interaction_surface_events_surface_created_idx
ON interaction_surface_events (surface_id, created_at ASC);
```

## Field Semantics

### channel

Identifies the channel runtime.

Examples:

```text
telegram
slack
web
email
mcp
```

The API treats this as a namespaced string, not as branching logic.

### surface_type

Describes the channel-visible role of the surface in generic terms.

Recommended initial values:

```text
current_materials_panel
analysis_task_surface
result_artifact_surface
subject_status_surface
subject_artifact_surface
```

Values may be channel-specific when needed, but they must not require API branching.

### surface_key

Stable key within owner, channel, and surface type. It prevents duplicate active surfaces for the same logical purpose while allowing the external address to change after a resend.

Recommended shapes:

```text
current_materials
analysis_run:{analysis_run_id}
artifact:{artifact_id}:result
```

The API treats `surface_key` as a string, but channel runtimes should keep it deterministic and idempotent.

### address

Opaque channel-owned external reference.

Telegram example:

```json
{
  "chat_id": "10",
  "thread_id": null,
  "message_id": "9010"
}
```

Slack example:

```json
{
  "channel_id": "C123",
  "message_ts": "1715860000.000100"
}
```

Email example:

```json
{
  "provider": "ses",
  "message_id": "abc"
}
```

The API stores this JSON but does not interpret transport-specific fields.

### address_fingerprint

Normalized fingerprint of `address`, computed either by the API or supplied by a trusted channel runtime according to the API contract.

Purpose:

- prevent two active surfaces from claiming the same external message;
- allow address lookup without indexing arbitrary JSON shape;
- avoid leaking channel-specific logic into domain entities.

Telegram fingerprint example:

```text
telegram:chat:10:thread:none:message:9010
```

### display_state

Opaque channel-owned metadata for recovery and optimistic rendering.

Telegram example:

```json
{
  "last_rendered_status": "running",
  "result_document_message_id": "9015",
  "superseded_reason": null
}
```

The API may enforce that `display_state` is valid JSON, but not its internal schema. MVP update semantics are replace-only with `expected_version`; merge semantics can be added later if there is a real need.

### subjects

Subjects point at API-owned domain records represented by a surface.

Recommended subject types:

```text
collection
selection
analysis_run
artifact
diagnostic
```

Recommended subject roles:

```text
primary
context
result
diagnostic
navigation_target
```

The API validates owner scope for every known subject. For example, an `analysis_run` subject must belong to the same owner as the surface. A surface must have exactly one `primary` subject.

Examples:

Current materials card:

```text
surface_type = current_materials_panel
surface_key = current_materials
subjects:
  collection / inbox_collection_id / primary
```

Task card:

```text
surface_type = analysis_task_surface
surface_key = analysis_run:{analysis_run_id}
subjects:
  analysis_run / analysis_run_id / primary
  selection / selection_id / context
```

Result file message:

```text
surface_type = result_artifact_surface
surface_key = artifact:{artifact_id}:result
subjects:
  artifact / artifact_id / primary
  analysis_run / analysis_run_id / context
```

### lifecycle_status

The lifecycle of the surface record itself, not the lifecycle of the represented domain subject.

Allowed values:

```text
active
superseded
deleted
failed
```

`active` means the channel runtime may try to edit or use the address.

`superseded` means the surface was replaced by another surface, usually because the external message could no longer be edited.

`deleted` means the API should not use this record for active recovery.

`failed` means the surface exists as an attempted representation, but the channel runtime could not create or refresh it.

## Internal API Contract

This should be an internal channel runtime API, not a public product API.

Recommended namespace:

```text
/internal/v1/interaction-surfaces
```

### Upsert Surface

```http
PUT /internal/v1/interaction-surfaces
```

Request:

```json
{
  "owner": {
    "owner_type": "telegram",
    "owner_id": "chat:10:user:7",
    "tenant_id": ""
  },
  "channel": "telegram",
  "surface_type": "analysis_task_surface",
  "surface_key": "analysis_run:run-123",
  "address": {
    "chat_id": "10",
    "message_id": "9010"
  },
  "subjects": [
    {
      "subject_type": "analysis_run",
      "subject_id": "run-123",
      "subject_role": "primary"
    },
    {
      "subject_type": "selection",
      "subject_id": "selection-777",
      "subject_role": "context"
    }
  ],
  "display_state": {
    "last_rendered_status": "queued"
  },
  "idempotency_key": "telegram:10:9010"
}
```

Response:

```json
{
  "surface": {
    "surface_id": "surface-1",
    "channel": "telegram",
    "surface_type": "analysis_task_surface",
    "surface_key": "analysis_run:run-123",
    "address": {
      "chat_id": "10",
      "message_id": "9010"
    },
    "subjects": [
      {
        "subject_type": "analysis_run",
        "subject_id": "run-123",
        "subject_role": "primary"
      },
      {
        "subject_type": "selection",
        "subject_id": "selection-777",
        "subject_role": "context"
      }
    ],
    "display_state": {
      "last_rendered_status": "queued"
    },
    "lifecycle_status": "active",
    "version": 1
  }
}
```

### Get Active Surface

```http
GET /internal/v1/interaction-surfaces/active?channel=telegram&surface_type=analysis_task_surface&surface_key=analysis_run%3Arun-123&owner_type=telegram&owner_id=chat%3A10%3Auser%3A7
```

Returns the active surface for that owner, channel, surface type, and surface key.

### List Surfaces

```http
GET /internal/v1/interaction-surfaces?channel=telegram&owner_type=telegram&owner_id=chat%3A10%3Auser%3A7&lifecycle_status=active&page_size=20
```

Useful for channel restart recovery.

Subject-scoped lookup:

```http
GET /internal/v1/interaction-surfaces?subject_type=analysis_run&subject_id=run-123&subject_role=primary&owner_type=telegram&owner_id=chat%3A10%3Auser%3A7
```

### Supersede Surface

```http
POST /internal/v1/interaction-surfaces/{surface_id}/supersede
```

Request:

```json
{
  "expected_version": 3,
  "reason": "message_not_editable"
}
```

Marks a surface as superseded without deleting history.

### Patch Surface Display State

```http
PATCH /internal/v1/interaction-surfaces/{surface_id}/display_state
```

Request:

```json
{
  "expected_version": 3,
  "display_state": {
    "last_rendered_status": "succeeded",
    "result_document_message_id": "9015"
  }
}
```

This replaces `display_state` when `expected_version` matches. Merge semantics are deferred until a concrete channel runtime needs them.

## Telegram Usage

Telegram uses interaction surfaces without requiring API Telegram awareness.

### Current Materials Card

Surface:

```text
channel = telegram
surface_type = current_materials_panel
surface_key = current_materials
subjects:
  collection / inbox_collection_id / primary
address = { chat_id, thread_id, message_id }
```

Behavior:

- after new media, find the active surface and edit it;
- if edit fails, supersede the old surface and create a new one;
- after starting transcription, clear the collection and render the current materials card as the latest message.

### Transcription Task Card

Surface:

```text
channel = telegram
surface_type = analysis_task_surface
surface_key = analysis_run:{analysis_run_id}
subjects:
  analysis_run / analysis_run_id / primary
  selection / selection_id / context
address = { chat_id, thread_id, message_id }
```

Behavior:

- created immediately after run creation;
- watcher edits this surface's message;
- on restart, channel lists active surfaces and resumes updating active analysis run cards.

### Result File Message

Surface:

```text
channel = telegram
surface_type = result_artifact_surface
surface_key = artifact:{artifact_id}:result
subjects:
  artifact / artifact_id / primary
  analysis_run / analysis_run_id / context
address = { chat_id, thread_id, message_id }
display_state = { analysis_run_id }
```

Behavior:

- created after the transcript file is sent;
- used to avoid duplicate file sends after restart;
- `Файл` can resend or navigate using this surface.

## Owner Scope And Security

Every interaction surface is owner-scoped.

Rules:

- channels can only create surfaces for owners they are authorized to act as;
- every known subject must match surface owner scope;
- a surface must have exactly one `primary` subject;
- the API stores `display_state`, but does not interpret channel button layout, rendered copy, or callback payloads;
- `surface_type` is stored as `TEXT` with API validation; do not use a database enum for MVP;
- `display_state` updates are replace-only and version-guarded for MVP;
- surface addresses are not public user-facing data;
- surface addresses may contain chat or provider identifiers and must be treated as operational metadata;
- public APIs must not expose interaction surface records to unrelated clients.

## Retention And Cleanup

Interaction surfaces should not outlive relevant domain history indefinitely.

Initial policy:

- active current-materials surfaces live until superseded or deleted;
- task-card surfaces live as long as their `analysis_run` history is retained;
- result-file surfaces live as long as their artifact history is retained;
- superseded surfaces can be pruned after an operational retention window;
- surface events follow the same retention as their parent surface unless audit policy requires longer diagnostic retention.

The exact retention window should be configured later with the rest of retention policy.

## Observability

The interaction surface layer should emit stable diagnostics or logs for:

- surface upsert;
- surface superseded;
- subject owner mismatch;
- channel recovery list;
- stale expected version;
- invalid subject type;
- invalid surface lifecycle_status;
- duplicate active surface key;
- duplicate active address fingerprint;
- display state version conflict.

Diagnostics should not include full secret URLs, bot tokens, or large opaque payloads.

## Implementation Stages

### Stage 1: Storage And Internal Contract

Goal: add generic interaction surface storage and internal API without changing domain behavior.

Files likely affected:

- `apps/api/internal/storage`
- `apps/api/internal/api`
- `apps/api/internal/storage/migrations`
- API tests
- GRACE docs

Verification:

- owner-scoped upsert/get/list/supersede/display-state tests;
- subject owner mismatch tests;
- idempotency tests;
- duplicate active surface key tests;
- duplicate active address fingerprint tests;
- event append tests;
- XML docs validation.

Non-goals:

- no Telegram UI rewrite yet;
- no Web or MCP usage;
- no public API exposure.

### Stage 2: Telegram Current Materials Surface

Goal: move Telegram current materials card message tracking from process memory to API interaction surfaces.

Telegram behavior:

- after accepted media, upsert or edit `current_materials_panel`;
- on edit failure, supersede and recreate;
- after restart, restore the current materials card surface.

Verification:

- Telegram channel tests for restart-like restore;
- current materials card stays recoverable after bot object recreation.

### Stage 3: Telegram Task Card Surface

Goal: create and update independent transcription task cards through interaction surface records.

Telegram behavior:

- create `analysis_task_surface` after run creation;
- watcher updates the task card surface;
- restart resumes active run card updates;
- current materials card remains independent.

Verification:

- running task does not block new current materials selection;
- watcher edits task card, not current selection card;
- restart test resumes task card tracking from the API surface list.

### Stage 4: Result File Surface

Goal: prevent duplicate result-file sends and support stable `Файл` behavior.

Telegram behavior:

- create `result_artifact_surface` after sending transcript document;
- if the surface already exists, avoid duplicate send unless user explicitly requests resend;
- task card can expose `Файл`.

Verification:

- completion after restart does not duplicate document messages;
- explicit `Файл` action works when result surface exists.

## Relationship To Telegram Card Contract

`docs/architecture/telegram-selection-transcription-cards.md` defines user-facing copy, buttons, and card behavior.

This document defines where the durable mapping for those cards lives and how it stays generic.

Together:

- Telegram card contract = presentation behavior;
- interaction surface contract = durable channel mapping;
- domain API = product state and execution lifecycle.

## Fixed Design Decisions

- `surface_type` is stored as `TEXT`; the API validates known core values and string shape, but the database does not use a PostgreSQL enum.
- `display_state` updates replace the full JSON document and require `expected_version`.
- Interaction surface records live in the main API database. Channel runtimes must not keep a separate durable store for these mappings.
- Superseded surfaces remain queryable for operational history and are pruned by retention policy later.
- Web does not use interaction surfaces until there is a concrete server-side recovery or sharing need; client-only UI state remains outside this contract.

## Remaining Open Questions

- What exact retention window should apply to superseded surfaces and surface events?
- Should address fingerprints be computed only by the API, or can trusted channel runtimes submit them when the API cannot normalize a future channel address?
