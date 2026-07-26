# Media Export and Retention Contract

Status: approved by intent, feasibility, verification, security/privacy, and data/ops review lanes for Bead `media-859r`.

## Product Intent

The user sends material once without selecting a mode. Every accepted input becomes a material in the current inbox. The same material can later participate in a combined processing run or in an explicit one-material export action without either workflow mutating the other.

Telegram remains one private chat. Web and Telegram use the same API-owned state.

## Source Classes and Identity

### Uploaded source

Telegram and Web uploads are durable source objects. The API streams each upload to an attempt-owned `staging/` key while computing SHA-256 and size, without buffering the whole body in process memory. Physical identity is `(channel_account_id, sha256, size_bytes)`, independent of filename, with a database unique constraint. The server validates the final byte count and digest before publication; a digest match with a different size is a collision and is rejected with a diagnostic.

Publication is race-safe:

1. upload bytes to a server-generated staging key;
2. validate digest, size, and media limits;
3. in one database transaction insert or select the canonical stored-object row and create the media occurrence plus inbox membership;
4. the winner promotes/copies the staging body to the deterministic managed key and marks the row `available`; a loser reuses the available canonical row and removes its staging object;
5. if database publication or promotion fails, the occurrence is not exposed as available and reconciliation removes the stale staging body after the orphan grace period.

Re-upload of a digest whose body is `deleted` creates a new storage generation under the same logical identity and moves the row through `publishing` to `available`; it never makes an expired body appear available before promotion succeeds. Liveness is derived transactionally from protected references and pins, not from an independently maintained reference counter.

Source expiry is anchored at `max(generation_published_at, last_successful_use_at)` and becomes eligible after `MEDIA_OBJECT_RETENTION_DAYS`, default `7`. Initial publication sets `generation_published_at` to ingestion time; re-publication of a deleted logical object advances it for the new storage generation while preserving original `created_at`. A never-used upload or re-upload therefore receives a full seven-day window. Metadata and immutable history remain after body expiry.

### Remote reference

A supported YouTube URL is a durable reference, not a durable media body. The API extracts a validated provider video id and stores a canonical HTTPS URL, bounded sanitized title, validated/proxied thumbnail metadata, duration, enrichment status, refresh timestamps, and safe diagnostics. Raw provider responses are never public. Enrichment never rewrites a sealed selection snapshot.

Workers reconstruct the canonical URL from the validated id and resolve current YouTube media for each processing or export attempt. Downloaded source bytes and extracted audio remain in bounded attempt scratch and are never registered as durable source objects.

### Generated delivery object

Audio or video produced for export is a transient managed object whose body follows `MEDIA_OBJECT_RETENTION_DAYS`, default `7`, independently from delivery retry and access windows. It persists long enough for restart-safe Telegram or Web delivery, retry, and audit lineage. If a channel already has the same verified SHA-256 and size, finalize reuses that canonical stored-object identity, extends its expiry monotonically, and attaches an independent delivery pin; the redundant attempt-owned promoted body is removed after the transaction or by reconciliation. Telegram resolves a private, container-reachable presigned URL through an authenticated internal API route, streams the response into an anonymous disk-backed file, and lets the Bot API consume that file in bounded chunks; the adapter never buffers a contract-valid export wholly in RAM. The anonymous handle is closed in `finally` and by the operating system on process exit. Telegram acknowledges delivery only after a successful send. Web link issuance is not an acknowledgement; Web access is issued for `EXPORT_WEB_ACCESS_TTL_HOURS` from successful completion and never extends the body beyond its media-retention expiry. Pending or failed delivery is bounded by `EXPORT_DELIVERY_TTL_HOURS` and `EXPORT_DELIVERY_MAX_ATTEMPTS`; expiry releases its retention pin without redefining the stored body's retention window.

## Processing Isolation

Telegram starts processing through one API operation:

`POST /v1/collections/{collection_id}/processing-runs`

The operation receives channel account, expected collection version, operation/idempotency key, run type, options, and the exact selected item ids. In one database transaction it validates scope and object availability, acquires source pins, creates and seals the immutable snapshot, creates the analysis run and step graph, and removes exactly the captured collection-item rows. New material arriving after the transaction remains in the inbox. Terminal result delivery never clears the live inbox.

The immutable snapshot and run retain retry history. Failure or cancellation does not return captured items to the live inbox automatically, but the user can retry the recorded run or explicitly restore its snapshot items. This preserves the requested immediate fresh inbox without losing the failed selection.

## Export Aggregate

`export_job` is a separate API-owned aggregate for exactly one media asset. Supported operations are:

- `youtube_audio`
- `youtube_video`
- `video_to_audio`

Raw provider format ids are worker details. Public variants are semantic values such as audio bitrate or bounded video quality.

### State machine

Job statuses are `queued`, `claimed`, `running`, `cancel_requested`, `succeeded`, `failed`, `canceled`, and `expired`.

- creation of a queued export atomically acquires non-expiring workload source pins only from currently `available` objects; those pins remain through queued and active states;
- `queued -> claimed` requires intact source pins, retry budget, a new `attempt_no`, random `attempt_token`, `lease_owner`, and `lease_expires_at`.
- `claimed -> running` and progress/heartbeat require owner plus attempt-token CAS and extend the lease.
- an expired `claimed` or `running` lease returns to `queued` if retry budget remains; otherwise it becomes `failed` with `export_retry_exhausted`.
- `queued -> canceled` is immediate and releases source pins. Active cancellation writes `cancel_requested`; the current fenced worker terminates the process group and finalizes `canceled`, releasing pins.
- `cancel_requested` wins over a late success/failure finalize. A stale owner or attempt token cannot publish progress, register output, or finalize.
- `running -> succeeded|failed|canceled` is one fenced transaction. Every terminal outcome releases source pins; only success advances `last_successful_use_at`. Success is allowed only after the output body is verified at its managed key, and records output lineage plus delivery retention.
- `failed|canceled -> queued` is an explicit idempotent retry operation that increments retry generation, never reuses an old attempt token, and atomically reacquires new workload pins only if every required uploaded source is still `available`; otherwise retry fails with `stored_object_unavailable` and remains terminal.
- terminal states do not transition except explicit retry from `failed` or `canceled`, or retention transition `succeeded -> expired` after output deletion.

Concurrent create with the same channel-scoped idempotency key and identical operation/variant replays the same job. Reuse with different input returns an idempotency conflict.

### Output publication crash cuts

The worker writes output to an attempt-owned transient staging key, validates digest/size, idempotently promotes it to an attempt-derived managed key, verifies the promoted body, and only then calls fenced finalize. Finalize creates an `available` output stored-object row and delivery row and marks the job `succeeded` in one transaction. A crash before promotion leaves staging bytes for the orphan reaper. A crash after promotion but before finalize leaves an unreferenced managed output that reconciliation removes after the orphan grace period. A crash after finalize is already coherent because the verified body precedes the terminal database state. A stale attempt can never attach its object to a reassigned job.

### Delivery state machine

Delivery states are `pending`, `claimed`, `delivered`, `failed`, and `expired`.

- an adapter claims delivery with owner, attempt token, and finite lease, then heartbeats that exact fence while a bounded download or Telegram upload is active;
- heartbeat extends but never shortens the current lease, cannot revive an expired or reassigned claim, and a lost heartbeat fence cancels the transfer path without acknowledgement;
- Telegram sends, then acknowledges with owner-token CAS; send failure moves `claimed -> failed`, increments attempts, and `failed -> pending` is an API-owned retry while attempts and TTL remain;
- an expired delivery lease is reclaimable until max attempts or TTL, then becomes `expired`;
- Telegram delivery is explicitly at-least-once. If the adapter crashes after Telegram accepts the send but before API acknowledgement, the expired claim is retried and may create one duplicate result message; the deterministic delivery id/result surface lets recovery recognize, record, and retire a duplicate best-effort. The system never marks an unacknowledged send as delivered or sacrifices eventual delivery to guess whether a send occurred;
- Web output access is account-scoped and uses a short-lived attachment URL; URL issuance does not mark delivered;
- delivery pins prevent output deletion until acknowledgement or TTL. Acknowledge/expiry atomically releases the pin before retention scheduling.

## Telegram Contract

The main processing card remains the stable current-materials surface and keeps `Обработать` as the primary action.

- A YouTube item row shows `Скачать` and `Убрать` in one keyboard row.
- An uploaded video row shows `В аудио` and `Убрать` in one keyboard row.
- When the current inbox contains exactly one eligible material, the main card also shows the matching contextual shortcut.
- Mixed inboxes expose contextual actions only on the material list.
- Starting, canceling, retrying, or delivering an export never removes collection membership and never clears the current processing card.
- Export progress uses a separate restart-safe `export_task_surface`.

## Workspace Contract

S3-compatible object storage is the durable byte authority. Telegram ingress uses an anonymous disk-backed temporary file only while transferring a source from Telegram to the streaming API upload; outbound Telegram export delivery uses the same anonymous-file property while bridging a private S3 stream into the Bot API. These handles have no durable pathname and the operating system releases them on close or process exit. Workers stream S3 bodies in bounded chunks into seekable attempt files and remove a partial destination after any transfer failure, so a large media object is never duplicated wholesale in process memory. Worker workspaces are bounded scratch for tools that require seekable files, including yt-dlp, ffmpeg, media concatenation, and the current CopperASR multipart client. Persisting those temporary copies in S3 would not remove the need for a seekable local file and would incorrectly turn ephemeral derived bytes into durable sources.

Every attempt owns a sanitized directory named from job/run id plus its fenced API attempt token where one exists, otherwise a random token. A local workspace marker is heartbeated for the lifetime of the attempt. The worker removes the directory in an outer `finally` after success, failure, or cancellation. A periodic reaper retries failed cleanup and removes only a workspace whose heartbeat is older than `WORKSPACE_ORPHAN_GRACE_MINUTES`; `WORKSPACE_ABSOLUTE_TTL_HOURS` is the final safety bound. Cleanup failure emits a diagnostic and metric but cannot rewrite an already terminal product result.

Tools run without a shell, with fixed argument vectors and server-generated paths. Canonical HTTPS provider URLs, redirect/egress policy, duration, input/output bytes, execution time, log size, local disk budget, and per-worker concurrency are bounded. Cancellation terminates the child process group. An attempt that would exceed its reserved workspace budget is rejected before download/conversion where metadata permits, and is aborted when actual bytes exceed the bound.

## Retention and Pinning

The API is the canonical retention authority. `stored_objects` have storage states `publishing`, `available`, `delete_scheduled`, `deleted`, and `missing`, plus retention state, generation, expiry, deletion owner/token/lease, attempts, and timestamps.

Analysis/export creation atomically inserts a protected source pin only while the object is `available` and not deletion-scheduled. Workload source pins do not expire: queued/active work owns them until a terminal transition releases them, so lease expiry cannot make active input deletable. A retry must reacquire new pins and revalidate availability. Delivery pins instead have the finite delivery/access TTL defined above. Retention scheduling locks/CASes the same row, proves expiry, absence of active source/delivery pins, and absence of an active hold, then changes it to `delete_scheduled`. A hold-versus-sweep race is serialized by that row lock; an active hold always blocks physical deletion. `delete_scheduled` blocks new pins and object access. This is the deletion fence; no database transaction remains open during S3 I/O.

The periodic sweeper:

1. claims a bounded batch with a random deletion token and finite lease;
2. deletes the managed object idempotently;
3. owner-token CAS records `deleted`, retention `expired`, and `deleted_at`, preserving metadata/history;
4. after an object-store error, records `retention_delete_failed`, increments attempts, and releases/requeues after bounded backoff;
5. after a crash following physical deletion, the stale claim is reclaimed, missing-object delete is treated as success, and database finalization completes;
6. every terminal analysis/export outcome releases its source pins; only success monotonically sets `last_successful_use_at=max(current, completed_at)` and recomputes expiry in that transaction. Failure and cancellation retain the prior expiry anchor.

Reconciliation is bidirectional and restricted to platform-managed prefixes:

- staging/transient bodies with no live database publication older than `OBJECT_ORPHAN_GRACE_MINUTES` are deleted;
- `publishing` rows are completed from their staging body or marked `missing` after the grace period;
- `available` rows whose bodies are absent are marked `missing`, dependent new work is rejected, and active work receives `stored_object_unavailable`;
- `delete_scheduled` rows with expired claims are reclaimed;
- all transitions are idempotent and fenced by generation/token, so inventory cannot delete an in-flight upload.

Reconciliation dry-run reports the same candidate counts but never mutates object state, delete fences, or database/object-list cursors. Repeated dry-runs therefore inspect the same persisted cursor position and cannot consume work from the live reconciler.

Bucket lifecycle rules may be used only as a safety backstop for explicit staging/transient prefixes with an age longer than application TTL/grace. They do not replace API-owned retention because bucket age cannot express active-run, delivery, or hold rules.

## Access and Provider Security

This is a single-user local product. Every retained host port carrying API, MinIO API/console, database, or private media state is explicitly bound to `127.0.0.1`; adapters use the private Compose network. The public API strictly validates the local Host allowlist and rejects every disallowed non-empty Origin before invoking a handler, including simple multipart requests, so loopback DNS rebinding and browser CSRF cannot mutate state. It must not be published on a non-loopback interface without authenticated identity mapping. Internal worker routes require `PLATFORM_INTERNAL_TOKEN`. The API resolves adapter identity to channel account server-side; public export/download DTOs never expose object-store bucket/key. Every output access rechecks account/job ownership and requires both job `succeeded` and output object `available`, then returns only a short-lived attachment response or URL.

Provider input is restricted to supported canonical HTTPS hosts. The API extracts and validates the video id, reconstructs the canonical URL, bounds redirects to allowed provider/CDN hosts, and rejects private/link-local destinations. User-supplied headers, cookies, proxies, output templates, playlists, live streams, DRM/account-cookie bypass, and silent quality downgrade are outside the contract. Titles strip markup/control characters and have a maximum length; thumbnails are validated/proxied; filenames are server-generated. Raw tool output, secrets, local paths, and signed URLs are redacted from diagnostics.

Users are responsible for having permission to download or transform source content.

## Configuration and Operations

Required defaults:

- `MEDIA_OBJECT_RETENTION_DAYS=7`
- `RETENTION_SWEEP_INTERVAL_SECONDS=300`
- `RETENTION_BATCH_SIZE=100`
- `RETENTION_CLAIM_SECONDS=120`
- `OBJECT_ORPHAN_GRACE_MINUTES=60`
- `EXPORT_DELIVERY_TTL_HOURS=24`
- `EXPORT_DELIVERY_MAX_ATTEMPTS=5`
- `EXPORT_WEB_ACCESS_TTL_HOURS=24`
- `WORKSPACE_ORPHAN_GRACE_MINUTES=30`
- `WORKSPACE_ABSOLUTE_TTL_HOURS=24`
- `MEDIA_EXPORT_MAX_DURATION_SECONDS=14400`
- `MEDIA_EXPORT_MAX_INPUT_BYTES=4294967296`
- `MEDIA_EXPORT_MAX_OUTPUT_BYTES=2147483648`
- `MEDIA_EXPORT_WORKSPACE_MAX_BYTES=6442450944`
- `MEDIA_EXPORT_TIMEOUT_SECONDS=1800`
- `MEDIA_EXPORT_CONCURRENCY=1`

Deployment order is additive migration, API with old behavior still supported, retention/export worker disabled, adapter/Web rollout, export worker enablement, then sweeper enablement after runtime proof. The API maintenance loop reclaims expired metadata-enrichment and export leases before reconciliation and physical sweeping, so worker or control-plane restarts cannot strand claimed work. A bounded metadata-only backfill may enqueue existing supported YouTube URL assets after canonicalizing one 11-character video id; it never copies provider media bytes and invalid references become terminal diagnostics. Legacy digest deduplication rewrites media, artifact, and historical snapshot locators to the canonical stored object before alias bodies are removed; forward repair migrations cover databases that already applied the earlier migration. These data migrations are explicitly forward-only. Rollback first disables claims and sweeps, drains or expires leases, rolls adapters/workers/API back, and leaves additive tables intact. Physical object deletion is irreversible; rollback cannot restore already-expired bytes.

Stable diagnostic and maintenance-log codes shipped by this packet include `export_provider_resolution_failed`, `export_lease_reclaimed`, `retention_claimed`, `retention_delete_failed`, `retention_reconciled_orphan`, `stored_object_missing`, and `workspace_cleanup_failed`. Export delivery failures are persisted as bounded `failure_code` values on fenced delivery attempts. Reclaim, reconciliation, and sweep APIs return structured counters, and maintenance logs expose their aggregate outcomes without tokens or object credentials. A dedicated metrics backend for queue age, retained/workspace bytes, and retry histograms is not part of this packet and must not be claimed as current runtime evidence.

## Verification Gates

- Schema/storage tests prove digest uniqueness, collision handling, staged publication crash cuts, concurrent losing-upload cleanup, deleted-body re-upload, pins, holds, lease fencing, hold-versus-sweep arbitration, physical deletion, missing-body reconciliation, and filename-independent byte reuse at the object-store level.
- API tests prove atomic seal/run/detach, expected-version conflict, retry from immutable snapshot, no terminal inbox clear, provider enrichment, export/delivery state machines, idempotency conflicts, account isolation, and short-lived output access.
- Deterministic race tests cover job start versus sweep, claim expiry/reclaim, stale finalize, cancel versus finalize, output upload versus finalize, delivery acknowledge versus sweep, object delete versus database finalize, and restart recovery.
- Worker tests prove attempt-unique workspace cleanup on success, failure, cancellation, process timeout, cleanup retry, heartbeat-aware orphan reaping, absolute TTL, resource limits, and that cleanup failure leaves terminal product state unchanged.
- Telegram tests prove contextual row and exact-single-item shortcuts, current-membership validation without collection-version coupling, separate current/export message surfaces, action-scoped export idempotency, private internal download access, anonymous file-backed delivery, claim/ack/failure retry, slow-job tracking, restart recovery, and no export mutation of processing collections.
- Web tests prove enriched rows, semantic quality selection, progress/cancel/retry, account-scoped download resolution, and access-window behavior.
- Security/runtime proof rejects disallowed Origin, hostile Host/DNS-rebinding, simple cross-origin multipart mutation, internal calls without service token, and verifies every private Compose host port binds to `127.0.0.1`.
- The isolated Compose export/retention proof uploads the same bytes under different names, observes one canonical S3 body, converts an uploaded video, downloads and acknowledges the delivery, exercises restart recovery and workspace cleanup, physically deletes only its retention-eligible UUID-scoped source body, and invokes non-destructive reconciliation. It selects the API and media-export worker plus their Compose dependencies; it does not select the metadata-enrichment worker and is not YouTube title-enrichment E2E evidence.
- YouTube title enrichment is verified separately by focused schema/API/worker/UI tests plus a dedicated live Compose metadata-enrichment smoke. That smoke is independent from the isolated export/retention E2E and must prove the metadata worker can resolve and finalize current title metadata without persisting provider source bytes.
- Required commands are `go test ./...` in `apps/api`, `uv run pytest` for Python workers/Telegram, Web tests/build, GRACE XML parsing, focused retention/export runtime script, full repository test gate, and Compose smoke.
