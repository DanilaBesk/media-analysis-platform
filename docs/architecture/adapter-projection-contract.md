# Adapter Projection Contract

Status: planned
Beads: media-cgr
Date: 2026-05-16

## Purpose

This document defines a generic API-owned projection layer for adapter-rendered presentation state.

The immediate driver is the Telegram card model in `docs/architecture/telegram-selection-transcription-cards.md`: Telegram needs durable mappings between current selection cards, transcription task cards, result file messages, and API-owned subjects. The API should provide durability and owner-scoped consistency without becoming aware of Telegram-specific UI concepts.

The design goal is:

- keep the domain API abstract and consumer-independent;
- avoid Telegram-specific fields on domain tables;
- support restart recovery for adapters;
- keep future Slack, email, Web, and mobile surfaces possible;
- separate durable business state from adapter presentation state.

## Simple Concept

An adapter projection is a durable record saying:

> Adapter X rendered subject Y as external surface Z.

The API stores the mapping. The adapter owns the meaning of the external reference and the visual rendering.

Example:

```json
{
  "adapter": "telegram",
  "projection_type": "analysis_run_card",
  "subject_type": "analysis_run",
  "subject_id": "run-123",
  "external_ref": {
    "chat_id": "10",
    "thread_id": null,
    "message_id": "9010"
  }
}
```

The API does not need to know that this is a Telegram message. It only knows that a projection exists for a subject and owner.

## Boundary Rules

### API Owns

- storing adapter projection records;
- owner-scope enforcement;
- idempotent upsert semantics;
- lookup by adapter, owner, subject, and projection type;
- lifecycle flags such as active, superseded, deleted;
- timestamps and optimistic versioning;
- retention-compatible cleanup rules.

### Adapter Owns

- rendering text, buttons, files, and UI details;
- interpreting `external_ref`;
- deciding whether to edit, resend, or supersede an external surface;
- adapter-specific status stored in opaque metadata;
- recovery behavior when an external message no longer exists.

### Domain API Owns

- media items;
- collections and inbox;
- immutable selections;
- analysis runs;
- artifacts;
- diagnostics;
- retention state.

The domain API must not depend on adapter projections.

## Non-Goals

- Do not add `telegram_message_id`, `telegram_chat_id`, or similar fields to `analysis_runs`, `collections`, `selections`, or `artifacts`.
- Do not create Telegram-specific API tables such as `telegram_cards`.
- Do not expose adapter projections as a public product feature in Web, MCP, or external contracts.
- Do not let projection state change domain lifecycle.
- Do not make the API interpret button layouts, card texts, or consumer UI states.

## Module Boundary

Recommended module:

```text
apps/api/internal/projections
```

or, if the current API organization prefers existing package grouping:

```text
apps/api/internal/storage/adapter_projections.go
apps/api/internal/api/adapter_projection_handlers.go
```

Conceptually this is a separate boundary:

```text
Domain API
  media_item / collection / selection / analysis_run / artifact / diagnostics

Adapter Projection API
  durable external rendering mappings
```

They can share one database and one API service, but their contracts must remain separate.

## Data Model

Recommended table:

```sql
CREATE TABLE adapter_projections (
  id TEXT PRIMARY KEY,
  owner_type TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  tenant_id TEXT,

  adapter TEXT NOT NULL,
  projection_type TEXT NOT NULL,

  subject_type TEXT NOT NULL,
  subject_id TEXT NOT NULL,

  external_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  state JSONB NOT NULL DEFAULT '{}'::jsonb,

  status TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  idempotency_key TEXT,

  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  superseded_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ
);
```

Recommended status values:

```text
active
superseded
deleted
failed
```

Recommended uniqueness:

```sql
CREATE UNIQUE INDEX adapter_projections_identity_idx
ON adapter_projections (
  owner_type,
  owner_id,
  COALESCE(tenant_id, ''),
  adapter,
  projection_type,
  subject_type,
  subject_id,
  COALESCE(idempotency_key, '')
)
```

Recommended active lookup index:

```sql
CREATE INDEX adapter_projections_active_idx
ON adapter_projections (
  owner_type,
  owner_id,
  COALESCE(tenant_id, ''),
  adapter,
  projection_type,
  subject_type,
  subject_id
)
WHERE status='active' AND deleted_at IS NULL;
```

## Field Semantics

### adapter

Identifies the adapter runtime.

Examples:

```text
telegram
slack
web
email
mcp
```

The API treats this as a namespaced string, not as branching logic.

### projection_type

Describes the adapter-visible role of the projection in generic terms.

Recommended initial values:

```text
current_selection_control
analysis_run_card
analysis_run_result_file
subject_status_surface
subject_artifact_surface
```

Values may be adapter-specific when needed, but they must not require API branching.

### subject_type / subject_id

Points at the domain subject being represented.

Recommended subject types:

```text
collection
selection
analysis_run
artifact
diagnostic
owner
```

The API validates owner scope where it can. For example, an `analysis_run` subject must belong to the owner.

### external_ref

Opaque adapter-owned external reference.

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

### state

Opaque adapter-owned metadata for recovery and optimistic rendering.

Telegram example:

```json
{
  "last_rendered_status": "running",
  "result_document_message_id": "9015",
  "superseded_reason": null
}
```

The API may enforce that `state` is valid JSON, but not its internal schema.

## Internal API Contract

This should be an internal adapter runtime API, not a public product API.

Recommended namespace:

```text
/internal/v1/adapter-projections
```

### Upsert Projection

```http
PUT /internal/v1/adapter-projections
```

Request:

```json
{
  "owner": {
    "owner_type": "telegram",
    "owner_id": "chat:10:user:7",
    "tenant_id": ""
  },
  "adapter": "telegram",
  "projection_type": "analysis_run_card",
  "subject_type": "analysis_run",
  "subject_id": "run-123",
  "external_ref": {
    "chat_id": "10",
    "message_id": "9010"
  },
  "state": {
    "last_rendered_status": "queued"
  },
  "idempotency_key": "telegram:10:9010"
}
```

Response:

```json
{
  "projection": {
    "projection_id": "projection-1",
    "adapter": "telegram",
    "projection_type": "analysis_run_card",
    "subject_type": "analysis_run",
    "subject_id": "run-123",
    "external_ref": {
      "chat_id": "10",
      "message_id": "9010"
    },
    "state": {
      "last_rendered_status": "queued"
    },
    "status": "active",
    "version": 1
  }
}
```

### Get Active Projection

```http
GET /internal/v1/adapter-projections/active?adapter=telegram&projection_type=analysis_run_card&subject_type=analysis_run&subject_id=run-123&owner_type=telegram&owner_id=chat%3A10%3Auser%3A7
```

Returns the latest active projection for that owner, adapter, projection type, and subject.

### List Projections

```http
GET /internal/v1/adapter-projections?adapter=telegram&owner_type=telegram&owner_id=chat%3A10%3Auser%3A7&status=active&page_size=20
```

Useful for adapter restart recovery.

### Supersede Projection

```http
POST /internal/v1/adapter-projections/{projection_id}/supersede
```

Request:

```json
{
  "expected_version": 3,
  "reason": "message_not_editable"
}
```

Marks a projection as superseded without deleting history.

### Patch Projection State

```http
PATCH /internal/v1/adapter-projections/{projection_id}/state
```

Request:

```json
{
  "expected_version": 3,
  "state": {
    "last_rendered_status": "succeeded",
    "result_document_message_id": "9015"
  }
}
```

The patch can either replace `state` or merge it. MVP should choose replacement for simpler deterministic tests.

## Telegram Usage

Telegram uses projections without requiring API Telegram awareness.

### Current Selection Card

Projection:

```text
adapter = telegram
projection_type = current_selection_control
subject_type = collection
subject_id = inbox collection id
external_ref = { chat_id, thread_id, message_id }
```

Behavior:

- after new media, find active projection and edit it;
- if edit fails, supersede old projection and create a new one;
- after starting transcription, clear the collection and render the current selection card as the latest message.

### Transcription Task Card

Projection:

```text
adapter = telegram
projection_type = analysis_run_card
subject_type = analysis_run
subject_id = analysis_run_id
external_ref = { chat_id, thread_id, message_id }
```

Behavior:

- created immediately after run creation;
- watcher edits this projection's message;
- on restart, adapter lists active projections and resumes updating active analysis run cards.

### Result File Message

Projection:

```text
adapter = telegram
projection_type = analysis_run_result_file
subject_type = artifact
subject_id = transcript artifact id
external_ref = { chat_id, thread_id, message_id }
state = { analysis_run_id }
```

Behavior:

- created after the transcript file is sent;
- used to avoid duplicate file sends after restart;
- `Файл` can resend or navigate using this projection.

## Owner Scope And Security

Every projection is owner-scoped.

Rules:

- adapters can only create projections for owners they are authorized to act as;
- subject owner must match projection owner when subject type is owner-scoped;
- external refs are not public user-facing data;
- external refs may contain chat or provider identifiers and must be treated as operational metadata;
- public APIs must not expose projection records to unrelated clients.

## Retention And Cleanup

Adapter projections should not outlive relevant domain history indefinitely.

Initial policy:

- active current selection projections live until superseded or deleted;
- task card projections live as long as their `analysis_run` history is retained;
- result file projections live as long as their artifact history is retained;
- superseded projections can be pruned after an operational retention window.

The exact retention window should be configured later with the rest of retention policy.

## Observability

The projection layer should emit stable diagnostics or logs for:

- projection upsert;
- projection superseded;
- subject owner mismatch;
- adapter recovery list;
- stale expected version;
- invalid subject type;
- invalid projection status.

Diagnostics should not include full secret URLs, bot tokens, or large opaque payloads.

## Implementation Stages

### Stage 1: Storage And Internal Contract

Goal: add generic projection storage and internal API without changing domain behavior.

Files likely affected:

- `apps/api/internal/storage`
- `apps/api/internal/api`
- `apps/api/internal/storage/migrations`
- API tests
- GRACE docs

Verification:

- owner-scoped upsert/get/list/supersede tests;
- subject owner mismatch tests;
- idempotency tests;
- XML docs validation.

Non-goals:

- no Telegram UI rewrite yet;
- no Web or MCP usage;
- no public API exposure.

### Stage 2: Telegram Current Selection Projection

Goal: move Telegram current selection card message tracking from process memory to API projections.

Telegram behavior:

- after accepted media, upsert or edit `current_selection_control`;
- on edit failure, supersede and recreate;
- after restart, restore the current selection card projection.

Verification:

- Telegram adapter tests for restart-like restore;
- current selection card stays recoverable after bot object recreation.

### Stage 3: Telegram Task Card Projection

Goal: create and update independent transcription task cards through projection records.

Telegram behavior:

- create `analysis_run_card` projection after run creation;
- watcher updates task card projection;
- restart resumes active run card updates;
- current selection card remains independent.

Verification:

- running task does not block new current selection;
- watcher edits task card, not current selection card;
- restart test resumes task card tracking from API projection list.

### Stage 4: Result File Projection

Goal: prevent duplicate result-file sends and support stable `Файл` behavior.

Telegram behavior:

- create `analysis_run_result_file` projection after sending transcript document;
- if projection already exists, avoid duplicate send unless user explicitly requests resend;
- task card can expose `Файл`.

Verification:

- completion after restart does not duplicate document messages;
- explicit `Файл` action works when result projection exists.

## Relationship To Telegram Card Contract

`docs/architecture/telegram-selection-transcription-cards.md` defines user-facing copy, buttons, and card behavior.

This document defines where the durable mapping for those cards lives and how it stays generic.

Together:

- Telegram card contract = presentation behavior;
- adapter projection contract = durable adapter mapping;
- domain API = product state and execution lifecycle.

## Open Questions

- Should `projection_type` be a controlled enum or adapter-defined string with validation only for length and characters?
- Should `state` replacement or merge semantics be used for PATCH?
- Should projection records be stored in the main API database only, or can adapters use this API while internal storage is still Postgres-owned?
- Which retention window should apply to superseded projections?
- Should Web use projections later, or should Web keep UI state purely client-side until a concrete need appears?
