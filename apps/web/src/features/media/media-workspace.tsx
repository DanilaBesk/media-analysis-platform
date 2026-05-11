import { useCallback, useEffect, useMemo, useState, type FormEvent, type ReactNode } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { RECONCILE_STATE_MARKER, requiresRestReconciliation } from "../../lib/api/client";
import type {
  AddMediaItemDraft,
  AnalysisRun,
  AnalysisRunSummary,
  Artifact,
  ArtifactSummary,
  Collection,
  Diagnostic,
  MediaItem,
  MediaItemSummary,
  ObservabilitySnapshot,
  OwnerScope,
  RunEvent,
  RunType,
} from "../../lib/api/types";
import { useWebUiRuntime } from "../../app/runtime-context";

const DEFAULT_OWNER: OwnerScope = {
  owner_type: "web",
  owner_id: "web-console",
};

const ACTIVE_RUN_STATUSES = new Set(["queued", "running", "cancel_requested"]);

interface DiagnosticSubject {
  subject_type: string;
  subject_id: string;
}

interface RunManifestItemOutcome {
  selection_item_id?: string;
  media_item_id: string;
  position: number;
  outcome: "succeeded" | "skipped" | "failed" | string;
  included?: boolean;
  artifact_kinds?: string[];
  diagnostic_ids?: string[];
  lineage?: {
    source_id?: string;
    role?: string;
    labels?: Record<string, unknown>;
  };
}

interface RunManifestPayload {
  schema_version?: string;
  summary?: {
    included_count?: number;
    skipped_count?: number;
    failed_count?: number;
  };
  items?: RunManifestItemOutcome[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseJsonObject(value?: string | null): Record<string, unknown> | null {
  if (!value?.trim()) {
    return null;
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function parseRunManifest(artifacts: ArtifactSummary[]): RunManifestPayload | null {
  const manifest = artifacts.find((artifact) => artifact.kind === "run_manifest");
  const parsed = parseJsonObject(manifest?.preview?.text_excerpt);
  if (!parsed) {
    return null;
  }
  return parsed as RunManifestPayload;
}

function diagnosticSubject(diagnostic: Diagnostic): DiagnosticSubject {
  const maybeLegacy = diagnostic as Diagnostic & {
    subject_type?: string;
    subject_id?: string;
  };
  return diagnostic.subject ?? {
    subject_type: maybeLegacy.subject_type ?? "analysis_run",
    subject_id: maybeLegacy.subject_id ?? "",
  };
}

function uniqueDiagnostics<TDiagnostic extends { diagnostic_id: string }>(
  diagnostics: TDiagnostic[],
): TDiagnostic[] {
  const seen = new Set<string>();
  return diagnostics.filter((diagnostic) => {
    if (seen.has(diagnostic.diagnostic_id)) {
      return false;
    }
    seen.add(diagnostic.diagnostic_id);
    return true;
  });
}

function diagnosticsForSubject(diagnostics: Diagnostic[], subjectId: string): Diagnostic[] {
  return diagnostics.filter((diagnostic) => diagnosticSubject(diagnostic).subject_id === subjectId);
}

function artifactGroupLabel(kind: ArtifactSummary["kind"]): string {
  switch (kind) {
    case "run_manifest":
      return "Run manifest";
    case "run_diagnostics":
    case "diagnostic_bundle":
      return "Diagnostics";
    case "structured_data":
      return "Structured data";
    case "execution_log":
      return "Execution logs";
    default:
      return kind.replace(/_/g, " ");
  }
}

function previewFormat(artifact: Artifact): "json" | "markdown" | "text" | "none" {
  const contentType = artifact.preview?.content_type ?? artifact.content_type;
  const format = artifact.preview?.format?.toLowerCase() ?? "";
  if (format.includes("json") || contentType.includes("json")) {
    return "json";
  }
  if (format.includes("markdown") || contentType.includes("markdown")) {
    return "markdown";
  }
  if (artifact.preview?.text_excerpt || contentType.startsWith("text/")) {
    return "text";
  }
  return "none";
}

function previewText(artifact: Artifact): string {
  const excerpt = artifact.preview?.text_excerpt ?? "";
  if (previewFormat(artifact) !== "json") {
    return excerpt;
  }
  const parsed = parseJsonObject(excerpt);
  return parsed ? JSON.stringify(parsed, null, 2) : excerpt;
}

function formatDate(value?: string | null): string {
  if (!value) {
    return "Not set";
  }
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatBytes(value?: number | null): string {
  if (!value) {
    return "0 B";
  }
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function compactId(value: string): string {
  if (value.length <= 14) {
    return value;
  }
  return `${value.slice(0, 8)}...${value.slice(-6)}`;
}

function runEventProgressText(event: RunEvent): string {
  if (event.event_type !== "analysis_run.progress" || !isRecord(event.payload)) {
    return "";
  }
  const stage = typeof event.payload.stage === "string" ? event.payload.stage : "";
  const message = typeof event.payload.message === "string" ? event.payload.message : "";
  return [stage, message].filter(Boolean).join(": ");
}

function sourceLabel(item: MediaItemSummary): string {
  if (item.source.external_uri) {
    return item.source.external_uri;
  }
  if (item.source.object_key) {
    return item.source.object_key;
  }
  if (item.source.text_ref) {
    return item.source.text_ref;
  }
  return item.source.origin_type;
}

function useMessage(): [string, (message: string) => void] {
  const [message, setMessage] = useState("");
  return [message, setMessage];
}

function useInboxData() {
  const { apiClient } = useWebUiRuntime();
  const [mediaItems, setMediaItems] = useState<MediaItemSummary[]>([]);
  const [inbox, setInbox] = useState<Collection | null>(null);
  const [collections, setCollections] = useState<Collection[]>([]);
  const [runs, setRuns] = useState<AnalysisRunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [mediaResponse, inboxResponse, collectionsResponse, runsResponse] = await Promise.all([
        apiClient.listMediaItems(DEFAULT_OWNER, { pageSize: 50 }),
        apiClient.getInboxCollection(DEFAULT_OWNER),
        apiClient.listCollections(DEFAULT_OWNER, { pageSize: 50 }),
        apiClient.listAnalysisRuns(DEFAULT_OWNER, { pageSize: 25 }),
      ]);
      setMediaItems(mediaResponse.items);
      setInbox(inboxResponse);
      setCollections(collectionsResponse.items);
      setRuns(runsResponse.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load the workspace.");
    } finally {
      setLoading(false);
    }
  }, [apiClient]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return {
    collections,
    error,
    inbox,
    loading,
    mediaItems,
    refresh,
    runs,
  };
}

function SectionHeader({
  eyebrow,
  title,
  action,
}: {
  eyebrow: string;
  title: string;
  action?: ReactNode;
}): JSX.Element {
  return (
    <div className="section-header">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h2>{title}</h2>
      </div>
      {action ? <div className="section-header__action">{action}</div> : null}
    </div>
  );
}

function MediaItemList({
  items,
  selected,
  onToggle,
  onSelectAll,
  onClearSelection,
  onRemove,
}: {
  items: MediaItemSummary[];
  selected: Set<string>;
  onToggle?: (mediaItemId: string) => void;
  onSelectAll?: () => void;
  onClearSelection?: () => void;
  onRemove?: (mediaItemId: string) => void;
}): JSX.Element {
  if (items.length === 0) {
    return <p className="muted-text">No media items match this owner.</p>;
  }

  return (
    <div className="data-list">
      {onToggle && onSelectAll && onClearSelection ? (
        <div className="selection-toolbar" aria-label="Selection controls">
          <button className="secondary-button" onClick={onSelectAll} type="button">
            Select all
          </button>
          <button className="secondary-button" disabled={selected.size === 0} onClick={onClearSelection} type="button">
            Clear selection
          </button>
        </div>
      ) : null}
      {items.map((item) => (
        <article className="data-row media-row" key={item.media_item_id}>
          {onToggle ? (
            <label className="select-cell">
              <input
                aria-label={`Select ${item.display_name}`}
                checked={selected.has(item.media_item_id)}
                onChange={() => onToggle(item.media_item_id)}
                type="checkbox"
              />
            </label>
          ) : null}
          <div className="row-main">
            <Link className="text-link" to={`/inbox/${item.media_item_id}`}>
              {item.display_name}
            </Link>
            <p className="muted-text">{sourceLabel(item)}</p>
          </div>
          <dl className="row-meta">
            <div>
              <dt>Kind</dt>
              <dd>{item.kind}</dd>
            </div>
            <div>
              <dt>Status</dt>
              <dd>
                <span className="status-pill" data-status={item.status}>
                  {item.status}
                </span>
              </dd>
            </div>
            <div>
              <dt>Added</dt>
              <dd>{formatDate(item.created_at)}</dd>
            </div>
          </dl>
          {onRemove ? (
            <button
              aria-label={`Soft delete ${item.display_name}`}
              className="icon-button danger"
              onClick={() => onRemove(item.media_item_id)}
              type="button"
            >
              Soft delete
            </button>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function IngestPanel({ onCreated }: { onCreated: () => Promise<void> }): JSX.Element {
  const { apiClient } = useWebUiRuntime();
  const [mode, setMode] = useState<"text" | "url" | "file">("text");
  const [displayName, setDisplayName] = useState("");
  const [text, setText] = useState("");
  const [url, setUrl] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useMessage();
  const [error, setError] = useState("");

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setPending(true);
    setError("");
    setMessage("");
    try {
      let draft: AddMediaItemDraft;
      if (mode === "text") {
        if (text.trim() === "") {
          throw new Error("Text is required.");
        }
        draft = {
          kind: "text",
          displayName: displayName.trim() || text.trim().slice(0, 64),
          adapterOrigin: "web",
          source: { origin_type: "text", text: text.trim() },
        };
      } else if (mode === "url") {
        if (url.trim() === "") {
          throw new Error("URL is required.");
        }
        draft = {
          kind: "url",
          displayName: displayName.trim() || url.trim(),
          adapterOrigin: "web",
          source: { origin_type: "url", url: url.trim() },
        };
      } else {
        if (!file) {
          throw new Error("Choose a file first.");
        }
        draft = {
          kind: file.type.startsWith("audio/")
            ? "audio"
            : file.type.startsWith("video/")
              ? "video"
              : file.type.startsWith("image/")
                ? "image"
                : "file",
          displayName: displayName.trim() || file.name,
          adapterOrigin: "web",
          source: {
            origin_type: "object",
            object_ref: `web-local://${encodeURIComponent(file.name)}-${file.size}-${file.lastModified}`,
            original_filename: file.name,
            content_type: file.type || "application/octet-stream",
            size_bytes: file.size,
          },
        };
      }
      const item = await apiClient.addMediaItem(DEFAULT_OWNER, draft);
      setMessage(`Added ${item.display_name}`);
      setDisplayName("");
      setText("");
      setUrl("");
      setFile(null);
      await onCreated();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to add media.");
    } finally {
      setPending(false);
    }
  };

  return (
    <section className="surface">
      <SectionHeader eyebrow="Ingest" title="Add media" />
      <form className="form-grid" onSubmit={(event) => void submit(event)}>
        <div className="segmented" role="group" aria-label="Ingest source">
          {(["text", "url", "file"] as const).map((entry) => (
            <button
              aria-pressed={mode === entry}
              className="segment-button"
              key={entry}
              onClick={() => setMode(entry)}
              type="button"
            >
              {entry === "file" ? "File/media" : entry.toUpperCase()}
            </button>
          ))}
        </div>
        <label>
          Display name
          <input value={displayName} onChange={(event) => setDisplayName(event.target.value)} />
        </label>
        {mode === "text" ? (
          <label>
            Text
            <textarea rows={5} value={text} onChange={(event) => setText(event.target.value)} />
          </label>
        ) : null}
        {mode === "url" ? (
          <label>
            URL
            <input value={url} onChange={(event) => setUrl(event.target.value)} />
          </label>
        ) : null}
        {mode === "file" ? (
          <label>
            File
            <input
              onChange={(event) => setFile(event.target.files?.[0] ?? null)}
              type="file"
            />
          </label>
        ) : null}
        <button disabled={pending} type="submit">
          {pending ? "Adding..." : "Add to inbox"}
        </button>
        {message ? <p className="success-text">{message}</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
      </form>
    </section>
  );
}

export function InboxRouteShell(): JSX.Element {
  const { apiClient } = useWebUiRuntime();
  const { collections, error, inbox, loading, mediaItems, refresh, runs } = useInboxData();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [collectionName, setCollectionName] = useState("");
  const [targetCollectionId, setTargetCollectionId] = useState("");
  const [actionPending, setActionPending] = useState(false);
  const [actionMessage, setActionMessage] = useMessage();
  const [actionError, setActionError] = useState("");

  const activeRuns = runs.filter((run) => ACTIVE_RUN_STATUSES.has(run.status));

  const toggle = (mediaItemId: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(mediaItemId)) {
        next.delete(mediaItemId);
      } else {
        next.add(mediaItemId);
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelected(new Set(mediaItems.map((item) => item.media_item_id)));
  };

  const clearSelection = () => {
    setSelected(new Set());
  };

  const createCollection = async () => {
    setActionPending(true);
    setActionError("");
    setActionMessage("");
    try {
      const collection = await apiClient.createCollection(DEFAULT_OWNER, {
        name: collectionName.trim() || `Selection ${new Date().toISOString().slice(0, 10)}`,
        items: Array.from(selected),
      });
      setSelected(new Set());
      setCollectionName("");
      setActionMessage(`Created ${collection.name}`);
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to create collection.");
    } finally {
      setActionPending(false);
    }
  };

  const addToCollection = async () => {
    const collection = collections.find((candidate) => candidate.collection_id === targetCollectionId);
    if (!collection) {
      setActionError("Choose a target collection.");
      return;
    }
    setActionPending(true);
    setActionError("");
    setActionMessage("");
    try {
      const existing = collection.items.map((item) => item.media_item_id);
      const merged = Array.from(new Set([...existing, ...selected]));
      await apiClient.replaceCollectionItems(DEFAULT_OWNER, collection.collection_id, {
        expectedVersion: collection.version,
        items: merged.map((media_item_id, position) => ({ media_item_id, position })),
      });
      setSelected(new Set());
      setActionMessage(`Updated ${collection.name}`);
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to update collection.");
    } finally {
      setActionPending(false);
    }
  };

  const removeMediaItem = async (mediaItemId: string) => {
    setActionPending(true);
    setActionError("");
    setActionMessage("");
    try {
      const removed = await apiClient.removeMediaItem(DEFAULT_OWNER, mediaItemId);
      setSelected((current) => {
        const next = new Set(current);
        next.delete(mediaItemId);
        return next;
      });
      setActionMessage(`Soft-deleted ${removed.display_name}`);
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to remove media.");
    } finally {
      setActionPending(false);
    }
  };

  return (
    <div className="page-grid page-grid--inbox">
      <section className="surface surface--main">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Refresh
            </button>
          }
          eyebrow="Inbox"
          title="Media inbox"
        />
        {loading ? <p className="muted-text">Loading inbox...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        {inbox ? (
          <div className="metric-strip">
            <div>
              <strong>{inbox.items.length}</strong>
              <span>in inbox</span>
            </div>
            <div>
              <strong>{mediaItems.length}</strong>
              <span>media items</span>
            </div>
            <div>
              <strong>{collections.length}</strong>
              <span>collections</span>
            </div>
            <div>
              <strong>{activeRuns.length}</strong>
              <span>active runs</span>
            </div>
          </div>
        ) : null}
        <MediaItemList
          items={mediaItems}
          onClearSelection={clearSelection}
          onRemove={(mediaItemId) => void removeMediaItem(mediaItemId)}
          onSelectAll={selectAll}
          onToggle={toggle}
          selected={selected}
        />
      </section>

      <aside className="side-stack">
        <IngestPanel onCreated={refresh} />
        <section className="surface">
          <SectionHeader eyebrow="Selection" title={`${selected.size} selected`} />
          <div className="form-grid">
            <label>
              New collection
              <input value={collectionName} onChange={(event) => setCollectionName(event.target.value)} />
            </label>
            <button disabled={actionPending || selected.size === 0} onClick={() => void createCollection()} type="button">
              Create collection
            </button>
            <label>
              Existing collection
              <select value={targetCollectionId} onChange={(event) => setTargetCollectionId(event.target.value)}>
                <option value="">Choose collection</option>
                {collections
                  .filter((collection) => collection.kind === "user" && collection.status === "active")
                  .map((collection) => (
                    <option key={collection.collection_id} value={collection.collection_id}>
                      {collection.name}
                    </option>
                  ))}
              </select>
            </label>
            <button disabled={actionPending || selected.size === 0} onClick={() => void addToCollection()} type="button">
              Add selected
            </button>
            {actionMessage ? <p className="success-text">{actionMessage}</p> : null}
            {actionError ? <p className="error-text">{actionError}</p> : null}
          </div>
        </section>
      </aside>
    </div>
  );
}

export function CollectionsRouteShell(): JSX.Element {
  const { apiClient } = useWebUiRuntime();
  const { collections, error, loading, mediaItems, refresh } = useInboxData();
  const [name, setName] = useState("");
  const [selectedMediaId, setSelectedMediaId] = useState("");
  const [addTargets, setAddTargets] = useState<Record<string, string>>({});
  const [message, setMessage] = useMessage();
  const [actionError, setActionError] = useState("");

  const userCollections = collections.filter((collection) => collection.kind === "user");

  const create = async () => {
    setActionError("");
    setMessage("");
    try {
      const collection = await apiClient.createCollection(DEFAULT_OWNER, {
        name: name.trim() || `Collection ${userCollections.length + 1}`,
        items: selectedMediaId ? [selectedMediaId] : [],
      });
      setMessage(`Created ${collection.name}`);
      setName("");
      setSelectedMediaId("");
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to create collection.");
    }
  };

  const rename = async (collection: Collection, nextName: string) => {
    if (nextName.trim() === "" || nextName === collection.name) {
      return;
    }
    setActionError("");
    try {
      await apiClient.updateCollection(DEFAULT_OWNER, collection.collection_id, {
        expectedVersion: collection.version,
        name: nextName,
      });
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to rename collection.");
    }
  };

  const removeItem = async (collection: Collection, mediaItemId: string) => {
    setActionError("");
    try {
      await apiClient.removeCollectionItem(
        DEFAULT_OWNER,
        collection.collection_id,
        mediaItemId,
        collection.version,
      );
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to remove item.");
    }
  };

  const addItem = async (collection: Collection) => {
    const mediaItemId = addTargets[collection.collection_id] ?? "";
    setActionError("");
    try {
      const existing = collection.items.map((item) => item.media_item_id);
      await apiClient.replaceCollectionItems(DEFAULT_OWNER, collection.collection_id, {
        expectedVersion: collection.version,
        items: [...existing, mediaItemId].map((item_id, position) => ({ media_item_id: item_id, position })),
      });
      setAddTargets((current) => ({ ...current, [collection.collection_id]: "" }));
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to add item.");
    }
  };

  const setArchiveState = async (collection: Collection, status: "archived" | "active") => {
    setActionError("");
    try {
      await apiClient.updateCollection(DEFAULT_OWNER, collection.collection_id, {
        expectedVersion: collection.version,
        status,
      });
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Unable to update collection.");
    }
  };

  return (
    <div className="page-grid">
      <section className="surface">
        <SectionHeader eyebrow="Collections" title="Manage collections" />
        <div className="form-grid form-grid--inline">
          <label>
            Name
            <input value={name} onChange={(event) => setName(event.target.value)} />
          </label>
          <label>
            First item
            <select value={selectedMediaId} onChange={(event) => setSelectedMediaId(event.target.value)}>
              <option value="">None</option>
              {mediaItems.map((item) => (
                <option key={item.media_item_id} value={item.media_item_id}>
                  {item.display_name}
                </option>
              ))}
            </select>
          </label>
          <button onClick={() => void create()} type="button">
            Create
          </button>
        </div>
        {message ? <p className="success-text">{message}</p> : null}
        {actionError ? <p className="error-text">{actionError}</p> : null}
      </section>

      <section className="surface surface--main">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Refresh
            </button>
          }
          eyebrow="Library"
          title="Collections list"
        />
        {loading ? <p className="muted-text">Loading collections...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        <div className="collection-grid">
          {userCollections.map((collection) => (
            <article className="surface surface--flat" key={collection.collection_id}>
              <div className="collection-title">
                <input
                  aria-label={`Rename ${collection.name}`}
                  defaultValue={collection.name}
                  onBlur={(event) => void rename(collection, event.target.value)}
                />
                <span className="status-pill" data-status={collection.status}>
                  {collection.status}
                </span>
              </div>
              <p className="muted-text">
                {collection.items.length} items, version {collection.version}
              </p>
              <div className="mini-list">
                {collection.items.map((item) => (
                  <div className="mini-row" key={item.media_item_id}>
                    <span>{item.media_item?.display_name ?? compactId(item.media_item_id)}</span>
                    <button
                      className="text-button danger"
                      onClick={() => void removeItem(collection, item.media_item_id)}
                      type="button"
                    >
                      Remove
                    </button>
                  </div>
                ))}
              </div>
              <div className="form-grid form-grid--compact">
                <label>
                  Add inbox item
                  <select
                    value={addTargets[collection.collection_id] ?? ""}
                    onChange={(event) =>
                      setAddTargets((current) => ({
                        ...current,
                        [collection.collection_id]: event.target.value,
                      }))
                    }
                  >
                    <option value="">Choose item</option>
                    {mediaItems
                      .filter((item) => !collection.items.some((entry) => entry.media_item_id === item.media_item_id))
                      .map((item) => (
                        <option key={item.media_item_id} value={item.media_item_id}>
                          {item.display_name}
                        </option>
                      ))}
                  </select>
                </label>
                <button
                  className="secondary-button"
                  disabled={!addTargets[collection.collection_id]}
                  onClick={() => void addItem(collection)}
                  type="button"
                >
                  Add item
                </button>
              </div>
              <div className="button-row">
                <button
                  className="secondary-button"
                  onClick={() => void setArchiveState(collection, collection.status === "archived" ? "active" : "archived")}
                  type="button"
                >
                  {collection.status === "archived" ? "Activate" : "Archive"}
                </button>
                <Link className="button-link" to={`/runs?collection=${collection.collection_id}`}>
                  Build run
                </Link>
              </div>
            </article>
          ))}
        </div>
      </section>
    </div>
  );
}

export function RunsRouteShell(): JSX.Element {
  const { apiClient } = useWebUiRuntime();
  const [searchParams] = useSearchParams();
  const { collections, error, loading, mediaItems, refresh, runs } = useInboxData();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [sourceCollectionId, setSourceCollectionId] = useState("");
  const [runType, setRunType] = useState<RunType>("transcription");
  const [paramsText, setParamsText] = useState("{\n  \"priority\": \"normal\"\n}");
  const [lastPlan, setLastPlan] = useState<{ selectionId: string; runId: string } | null>(null);
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useMessage();
  const [runError, setRunError] = useState("");

  const collectionItems = useMemo(() => {
    const collection = collections.find((candidate) => candidate.collection_id === sourceCollectionId);
    return collection?.items.map((item) => item.media_item_id) ?? [];
  }, [collections, sourceCollectionId]);

  const selectedItems = useMemo(
    () => mediaItems.filter((item) => selected.has(item.media_item_id)),
    [mediaItems, selected],
  );

  useEffect(() => {
    const collectionFromQuery = searchParams.get("collection") ?? "";
    if (!collectionFromQuery || sourceCollectionId) {
      return;
    }
    if (collections.some((collection) => collection.collection_id === collectionFromQuery)) {
      setSourceCollectionId(collectionFromQuery);
    }
  }, [collections, searchParams, sourceCollectionId]);

  useEffect(() => {
    if (sourceCollectionId) {
      setSelected(new Set(collectionItems));
    }
  }, [collectionItems, sourceCollectionId]);

  const toggle = (mediaItemId: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(mediaItemId)) {
        next.delete(mediaItemId);
      } else {
        next.add(mediaItemId);
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelected(new Set(mediaItems.map((item) => item.media_item_id)));
  };

  const clearSelection = () => {
    setSelected(new Set());
  };

  const createRun = async () => {
    setPending(true);
    setRunError("");
    setMessage("");
    try {
      const ids = Array.from(selected);
      const params = paramsText.trim() ? (JSON.parse(paramsText) as Record<string, unknown>) : undefined;
      const selection = await apiClient.createSelection(DEFAULT_OWNER, {
        sourceCollectionId: sourceCollectionId || undefined,
        items: ids.map((media_item_id, position) => ({ media_item_id, position })),
        optionSnapshot: { source: sourceCollectionId ? "collection" : "manual" },
        duplicatePolicy: "reject",
        createdBy: "web",
      });
      const run = await apiClient.createAnalysisRun(DEFAULT_OWNER, {
        selectionId: selection.selection_id,
        runType,
        params,
        delivery: { strategy: "polling" },
      });
      setLastPlan({ selectionId: selection.selection_id, runId: run.analysis_run_id });
      setMessage(`Run queued: ${compactId(run.analysis_run_id)}`);
      await refresh();
    } catch (err) {
      setRunError(err instanceof Error ? err.message : "Unable to create run.");
    } finally {
      setPending(false);
    }
  };

  return (
    <div className="page-grid page-grid--builder">
      <section className="surface surface--main">
        <SectionHeader eyebrow="Run builder" title="Create immutable selection" />
        <div className="form-grid form-grid--inline">
          <label>
            Collection
            <select value={sourceCollectionId} onChange={(event) => setSourceCollectionId(event.target.value)}>
              <option value="">Manual selection</option>
              {collections
                .filter((collection) => collection.kind === "user" && collection.status === "active")
                .map((collection) => (
                  <option key={collection.collection_id} value={collection.collection_id}>
                    {collection.name}
                  </option>
                ))}
            </select>
          </label>
          <label>
            Run type
            <select value={runType} onChange={(event) => setRunType(event.target.value as RunType)}>
              <option value="transcription">Transcription</option>
              <option value="summary">Summary</option>
              <option value="report">Report</option>
              <option value="deep_research">Deep research</option>
              <option value="custom">Custom</option>
            </select>
          </label>
          <label>
            Params
            <textarea rows={4} value={paramsText} onChange={(event) => setParamsText(event.target.value)} />
          </label>
        </div>
        <MediaItemList
          items={mediaItems}
          onClearSelection={clearSelection}
          onSelectAll={selectAll}
          onToggle={toggle}
          selected={selected}
        />
        <div className="button-row">
          <button disabled={pending || selected.size === 0} onClick={() => void createRun()} type="button">
            {pending ? "Creating..." : `Create run from ${selected.size} items`}
          </button>
          {message ? <p className="success-text">{message}</p> : null}
          {runError ? <p className="error-text">{runError}</p> : null}
        </div>
      </section>

      <section className="surface">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Refresh
            </button>
          }
          eyebrow="Runs"
          title="Recent runs"
        />
        {loading ? <p className="muted-text">Loading runs...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        <RunList runs={runs} />
      </section>
      <section className="surface">
        <SectionHeader eyebrow="Frozen plan" title={`${selected.size} sources`} />
        <dl className="detail-grid detail-grid--single">
          <div>
            <dt>Source</dt>
            <dd>
              {sourceCollectionId
                ? collections.find((collection) => collection.collection_id === sourceCollectionId)?.name ?? compactId(sourceCollectionId)
                : "Manual selection"}
            </dd>
          </div>
          <div>
            <dt>Run type</dt>
            <dd>{runType}</dd>
          </div>
          <div>
            <dt>Selection mode</dt>
            <dd>sealed before queue</dd>
          </div>
        </dl>
        {selectedItems.length > 0 ? (
          <div className="mini-list">
            {selectedItems.map((item, index) => (
              <div className="mini-row" key={item.media_item_id}>
                <span>
                  #{index + 1} {item.display_name}
                </span>
                <span className="muted-text">{item.kind}</span>
              </div>
            ))}
          </div>
        ) : (
          <p className="muted-text">Select media or choose a collection.</p>
        )}
        {lastPlan ? (
          <dl className="detail-grid detail-grid--single">
            <div>
              <dt>Last selection</dt>
              <dd>{compactId(lastPlan.selectionId)}</dd>
            </div>
            <div>
              <dt>Last run</dt>
              <dd>
                <Link className="text-link" to={`/runs/${lastPlan.runId}`}>
                  {compactId(lastPlan.runId)}
                </Link>
              </dd>
            </div>
          </dl>
        ) : null}
      </section>
    </div>
  );
}

function RunList({ runs }: { runs: AnalysisRunSummary[] }): JSX.Element {
  if (runs.length === 0) {
    return <p className="muted-text">No runs for this owner.</p>;
  }
  return (
    <div className="data-list">
      {runs.map((run) => (
        <article className="data-row" key={run.analysis_run_id}>
          <div className="row-main">
            <Link className="text-link" to={`/runs/${run.analysis_run_id}`}>
              {run.run_type}
            </Link>
            <p className="muted-text">{compactId(run.analysis_run_id)}</p>
          </div>
          <dl className="row-meta">
            <div>
              <dt>Status</dt>
              <dd>
                <span className="status-pill" data-status={run.status}>
                  {run.status}
                </span>
              </dd>
            </div>
            <div>
              <dt>Artifacts</dt>
              <dd>{run.artifact_count ?? 0}</dd>
            </div>
            <div>
              <dt>Created</dt>
              <dd>{formatDate(run.created_at)}</dd>
            </div>
          </dl>
        </article>
      ))}
    </div>
  );
}

export function RunDetailRouteShell(): JSX.Element {
  const { analysisRunId = "" } = useParams();
  const { apiClient } = useWebUiRuntime();
  const [run, setRun] = useState<AnalysisRun | null>(null);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [artifacts, setArtifacts] = useState<ArtifactSummary[]>([]);
  const [diagnostics, setDiagnostics] = useState<Diagnostic[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [message, setMessage] = useMessage();

  const refresh = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [runResponse, eventsResponse, artifactsResponse] = await Promise.all([
        apiClient.getAnalysisRun(DEFAULT_OWNER, analysisRunId),
        apiClient.listAnalysisRunEvents(DEFAULT_OWNER, analysisRunId, { pageSize: 50 }),
        apiClient.listArtifacts(DEFAULT_OWNER, { analysisRunId, pageSize: 50 }),
      ]);
      const subjects: DiagnosticSubject[] = [
        { subject_type: "analysis_run", subject_id: analysisRunId },
        { subject_type: "selection", subject_id: runResponse.selection_id },
        ...runResponse.selection.items.flatMap((item) => [
          { subject_type: "media_item", subject_id: item.media_item_id },
          { subject_type: "source", subject_id: item.source_snapshot.source_id },
        ]),
      ].filter((subject) => subject.subject_id);
      const diagnosticResponses = await Promise.all(
        subjects.map((subject) =>
          apiClient.listDiagnostics(DEFAULT_OWNER, {
            subjectType: subject.subject_type as never,
            subjectId: subject.subject_id,
            pageSize: 50,
          }),
        ),
      );
      setRun(runResponse);
      setEvents(eventsResponse.items);
      setArtifacts(artifactsResponse.items);
      setDiagnostics(uniqueDiagnostics(diagnosticResponses.flatMap((response) => response.items)));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load run.");
    } finally {
      setLoading(false);
    }
  }, [analysisRunId, apiClient]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    const subscription = apiClient.subscribeToRunEvents({
      onMessage(event) {
        if (event.analysis_run_id !== analysisRunId) {
          return;
        }
        const lastVersion = run?.version ?? 0;
        if (requiresRestReconciliation(lastVersion, event.version)) {
          console.info("%s analysis_run_id=%s", RECONCILE_STATE_MARKER, analysisRunId);
        }
        void refresh();
      },
    });
    return () => subscription.close();
  }, [analysisRunId, apiClient, refresh, run?.version]);

  const cancel = async () => {
    setMessage("");
    try {
      const next = await apiClient.cancelAnalysisRun(DEFAULT_OWNER, analysisRunId);
      setRun(next);
      setMessage("Cancel requested");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to cancel run.");
    }
  };

  const retry = async () => {
    setMessage("");
    try {
      const next = await apiClient.retryAnalysisRun(DEFAULT_OWNER, analysisRunId);
      setMessage(`Retry queued: ${compactId(next.analysis_run_id)}`);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to retry run.");
    }
  };

  const manifest = parseRunManifest([...(run?.artifacts ?? []), ...artifacts]);

  return (
    <div className="page-grid page-grid--detail">
      <section className="surface surface--main">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Refresh
            </button>
          }
          eyebrow="Run detail"
          title={run ? run.run_type : compactId(analysisRunId)}
        />
        {loading ? <p className="muted-text">Loading run...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        {message ? <p className="success-text">{message}</p> : null}
        {run ? (
          <>
            <dl className="detail-grid">
              <div>
                <dt>Status</dt>
                <dd>
                  <span className="status-pill" data-status={run.status}>
                    {run.status}
                  </span>
                </dd>
              </div>
              <div>
                <dt>Selection</dt>
                <dd>{compactId(run.selection_id)}</dd>
              </div>
              <div>
                <dt>Version</dt>
                <dd>{run.version}</dd>
              </div>
              <div>
                <dt>Evidence gate</dt>
                <dd>{run.evidence_gate_state}</dd>
              </div>
              <div>
                <dt>Created</dt>
                <dd>{formatDate(run.created_at)}</dd>
              </div>
              <div>
                <dt>Completed</dt>
                <dd>{formatDate(run.completed_at)}</dd>
              </div>
            </dl>
            <div className="button-row">
              <button disabled={!ACTIVE_RUN_STATUSES.has(run.status)} onClick={() => void cancel()} type="button">
                Cancel
              </button>
              <button className="secondary-button" onClick={() => void retry()} type="button">
                Retry
              </button>
            </div>
            <section className="subsection">
              <h3>Selection snapshot</h3>
              <div className="mini-list">
                {run.selection.items.map((item) => (
                  <div className="mini-row" key={`${item.media_item_id}-${item.position}`}>
                    <span>
                      #{item.position + 1} {item.display_name}
                    </span>
                    <span className="muted-text">{item.kind}</span>
                  </div>
                ))}
              </div>
            </section>
            <section className="subsection">
              <h3>Item outcomes</h3>
              <RunOutcomeList run={run} manifest={manifest} diagnostics={diagnostics} />
            </section>
            <section className="subsection">
              <h3>Source diagnostics</h3>
              <SourceDiagnosticsList run={run} diagnostics={diagnostics} />
            </section>
          </>
        ) : null}
      </section>

      <aside className="side-stack">
        <section className="surface">
          <SectionHeader eyebrow="Events" title="Run events" />
          <EventList events={events} />
        </section>
        <section className="surface">
          <SectionHeader eyebrow="Artifacts" title="Run artifacts" />
          <ArtifactList artifacts={artifacts} />
        </section>
        <section className="surface">
          <SectionHeader eyebrow="Diagnostics" title="Run diagnostics" />
          <DiagnosticList diagnostics={diagnostics} />
        </section>
      </aside>
    </div>
  );
}

function RunOutcomeList({
  run,
  manifest,
  diagnostics,
}: {
  run: AnalysisRun;
  manifest: RunManifestPayload | null;
  diagnostics: Diagnostic[];
}): JSX.Element {
  if (manifest?.items?.length) {
    return (
      <div className="outcome-table">
        {manifest.summary ? (
          <div className="metric-strip metric-strip--compact">
            <div>
              <strong>{manifest.summary.included_count ?? 0}</strong>
              <span>included</span>
            </div>
            <div>
              <strong>{manifest.summary.skipped_count ?? 0}</strong>
              <span>skipped</span>
            </div>
            <div>
              <strong>{manifest.summary.failed_count ?? 0}</strong>
              <span>failed</span>
            </div>
          </div>
        ) : null}
        <div className="data-list">
          {manifest.items.map((item) => {
            const sourceId = item.lineage?.source_id ?? "";
            return (
              <article className="data-row outcome-row" key={`${item.media_item_id}-${item.position}`}>
                <div className="row-main">
                  <strong>
                    #{item.position + 1} {compactId(item.media_item_id)}
                  </strong>
                  <p className="muted-text">
                    {sourceId ? `source ${compactId(sourceId)}` : item.selection_item_id ? `selection ${compactId(item.selection_item_id)}` : "selection item"}
                  </p>
                </div>
                <dl className="row-meta">
                  <div>
                    <dt>Outcome</dt>
                    <dd>
                      <span className="status-pill" data-status={item.outcome}>
                        {item.outcome}
                      </span>
                    </dd>
                  </div>
                  <div>
                    <dt>Artifacts</dt>
                    <dd>{item.artifact_kinds?.join(", ") || "None"}</dd>
                  </div>
                  <div>
                    <dt>Diagnostics</dt>
                    <dd>{item.diagnostic_ids?.length ?? diagnosticsForSubject(diagnostics, item.media_item_id).length}</dd>
                  </div>
                </dl>
              </article>
            );
          })}
        </div>
      </div>
    );
  }

  return (
    <div className="data-list">
      {run.selection.items.map((item) => {
        const itemDiagnostics = diagnosticsForSubject(diagnostics, item.media_item_id);
        const sourceDiagnostics = diagnosticsForSubject(diagnostics, item.source_snapshot.source_id);
        return (
          <article className="data-row outcome-row" key={`${item.media_item_id}-${item.position}`}>
            <div className="row-main">
              <strong>
                #{item.position + 1} {item.display_name}
              </strong>
              <p className="muted-text">{compactId(item.media_item_id)}</p>
            </div>
            <dl className="row-meta">
              <div>
                <dt>Selected as</dt>
                <dd>{item.status_at_selection}</dd>
              </div>
              <div>
                <dt>Source</dt>
                <dd>{compactId(item.source_snapshot.source_id)}</dd>
              </div>
              <div>
                <dt>Diagnostics</dt>
                <dd>{itemDiagnostics.length + sourceDiagnostics.length}</dd>
              </div>
            </dl>
          </article>
        );
      })}
    </div>
  );
}

function SourceDiagnosticsList({
  run,
  diagnostics,
}: {
  run: AnalysisRun;
  diagnostics: Diagnostic[];
}): JSX.Element {
  const entries = run.selection.items.map((item) => {
    const itemDiagnostics = diagnosticsForSubject(diagnostics, item.media_item_id);
    const sourceDiagnostics = diagnosticsForSubject(diagnostics, item.source_snapshot.source_id);
    return { item, diagnostics: [...itemDiagnostics, ...sourceDiagnostics] };
  });

  if (entries.every((entry) => entry.diagnostics.length === 0)) {
    return <p className="muted-text">No source-level diagnostics.</p>;
  }

  return (
    <div className="data-list">
      {entries
        .filter((entry) => entry.diagnostics.length > 0)
        .map((entry) => (
          <article className="data-row source-diagnostic-row" key={entry.item.media_item_id}>
            <div className="row-main">
              <strong>{entry.item.display_name}</strong>
              <p className="muted-text">
                media {compactId(entry.item.media_item_id)} / source {compactId(entry.item.source_snapshot.source_id)}
              </p>
            </div>
            <DiagnosticList diagnostics={entry.diagnostics} />
          </article>
        ))}
    </div>
  );
}

function EventList({ events }: { events: RunEvent[] }): JSX.Element {
  if (events.length === 0) {
    return <p className="muted-text">No events recorded.</p>;
  }
  return (
    <div className="timeline-list">
      {events.map((event) => (
        <article className="timeline-entry" key={event.event_id}>
          <strong>{event.event_type}</strong>
          <span className="muted-text">version {event.version}</span>
          <span className="muted-text">{formatDate(event.emitted_at)}</span>
          {runEventProgressText(event) ? (
            <span className="muted-text">{runEventProgressText(event)}</span>
          ) : null}
          {event.status ? (
            <span className="status-pill" data-status={event.status}>
              {event.status}
            </span>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function ArtifactList({ artifacts }: { artifacts: ArtifactSummary[] }): JSX.Element {
  if (artifacts.length === 0) {
    return <p className="muted-text">No artifacts available.</p>;
  }
  return (
    <div className="data-list">
      {artifacts.map((artifact) => (
        <article className="data-row" key={artifact.artifact_id}>
          <div className="row-main">
            <Link className="text-link" to={`/artifacts/${artifact.artifact_id}`}>
              {artifact.kind}
            </Link>
            <p className="muted-text">{artifact.content_type}</p>
          </div>
          <dl className="row-meta">
            <div>
              <dt>Status</dt>
              <dd>{artifact.status}</dd>
            </div>
            <div>
              <dt>Size</dt>
              <dd>{formatBytes(artifact.size_bytes)}</dd>
            </div>
          </dl>
        </article>
      ))}
    </div>
  );
}

function GroupedArtifactList({ artifacts }: { artifacts: ArtifactSummary[] }): JSX.Element {
  if (artifacts.length === 0) {
    return <p className="muted-text">No artifacts available.</p>;
  }

  const groups = artifacts.reduce<Record<string, ArtifactSummary[]>>((acc, artifact) => {
    const key = `${artifact.analysis_run_id}:${artifactGroupLabel(artifact.kind)}`;
    acc[key] = [...(acc[key] ?? []), artifact];
    return acc;
  }, {});

  return (
    <div className="artifact-groups">
      {Object.entries(groups).map(([key, group]) => {
        const [analysisRunId, label] = key.split(":");
        return (
          <section className="artifact-group" key={key}>
            <div className="artifact-group__header">
              <strong>{label}</strong>
              <Link className="text-link" to={`/runs/${analysisRunId}`}>
                {compactId(analysisRunId)}
              </Link>
            </div>
            <ArtifactList artifacts={group} />
          </section>
        );
      })}
    </div>
  );
}

function DiagnosticList({ diagnostics }: { diagnostics: Diagnostic[] }): JSX.Element {
  if (diagnostics.length === 0) {
    return <p className="muted-text">No diagnostics.</p>;
  }
  return (
    <div className="data-list">
      {diagnostics.map((diagnostic) => (
        <article className="data-row diagnostic-row" key={diagnostic.diagnostic_id}>
          <div className="row-main">
            <strong>{diagnostic.code}</strong>
            <p className="muted-text">{diagnostic.message}</p>
            <p className="muted-text">
              {diagnosticSubject(diagnostic).subject_type} {compactId(diagnosticSubject(diagnostic).subject_id)}
            </p>
          </div>
          <span className="status-pill" data-status={diagnostic.severity}>
            {diagnostic.severity}
          </span>
        </article>
      ))}
    </div>
  );
}

export function ArtifactsRouteShell(): JSX.Element {
  const { artifactId = "" } = useParams();
  const { apiClient } = useWebUiRuntime();
  const [artifacts, setArtifacts] = useState<ArtifactSummary[]>([]);
  const [artifact, setArtifact] = useState<Artifact | null>(null);
  const [diagnostics, setDiagnostics] = useState<Diagnostic[]>([]);
  const [error, setError] = useState("");
  const [message, setMessage] = useMessage();
  const [refreshingArtifact, setRefreshingArtifact] = useState(false);

  const refresh = useCallback(async () => {
    setError("");
    try {
      const response = await apiClient.listArtifacts(DEFAULT_OWNER, { pageSize: 50 });
      setArtifacts(response.items);
      if (artifactId) {
        const [artifactResponse, diagnosticsResponse] = await Promise.all([
          apiClient.getArtifact(DEFAULT_OWNER, artifactId),
          apiClient.listDiagnostics(DEFAULT_OWNER, {
            subjectType: "artifact",
            subjectId: artifactId,
            pageSize: 50,
          }),
        ]);
        setArtifact(artifactResponse);
        setDiagnostics(diagnosticsResponse.items);
      } else {
        setArtifact(null);
        setDiagnostics([]);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load artifacts.");
    }
  }, [apiClient, artifactId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const artifactDiagnostics = artifact
    ? uniqueDiagnostics([...(artifact.diagnostics ?? []), ...diagnostics] as Diagnostic[])
    : [];

  const refreshArtifact = async () => {
    setError("");
    setMessage("");
    setRefreshingArtifact(true);
    try {
      const refreshed = await apiClient.refreshArtifact(DEFAULT_OWNER, artifactId);
      setArtifact(refreshed);
      setMessage("Artifact access refreshed");
      const artifactsResponse = await apiClient.listArtifacts(DEFAULT_OWNER, { pageSize: 50 });
      setArtifacts(artifactsResponse.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to refresh artifact access.");
    } finally {
      setRefreshingArtifact(false);
    }
  };

  return (
    <div className="page-grid">
      <section className="surface surface--main">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Refresh
            </button>
          }
          eyebrow="Artifacts"
          title="Artifact browser"
        />
        {error ? <p className="error-text">{error}</p> : null}
        {message ? <p className="success-text">{message}</p> : null}
        <GroupedArtifactList artifacts={artifacts} />
      </section>
      <section className="surface">
        <SectionHeader eyebrow="Preview" title={artifact ? artifact.kind : "Select artifact"} />
        {artifact ? (
          <div className="artifact-preview">
            <dl className="detail-grid">
              <div>
                <dt>Status</dt>
                <dd>{artifact.status}</dd>
              </div>
              <div>
                <dt>Content</dt>
                <dd>{artifact.content_type}</dd>
              </div>
              <div>
                <dt>Size</dt>
                <dd>{formatBytes(artifact.size_bytes)}</dd>
              </div>
              <div>
                <dt>Run</dt>
                <dd>
                  <Link className="text-link" to={`/runs/${artifact.analysis_run_id}`}>
                    {compactId(artifact.analysis_run_id)}
                  </Link>
                </dd>
              </div>
              <div>
                <dt>Format</dt>
                <dd>{artifact.preview?.format ?? previewFormat(artifact)}</dd>
              </div>
            </dl>
            {previewFormat(artifact) !== "none" && previewText(artifact) ? (
              <pre data-format={previewFormat(artifact)}>{previewText(artifact)}</pre>
            ) : (
              <p className="muted-text">No inline preview is available for this artifact.</p>
            )}
            {(artifact.download?.available ?? Boolean(artifact.download?.url)) && artifact.download?.url ? (
              <a className="button-link" href={artifact.download.url} rel="noreferrer" target="_blank">
                Open artifact
              </a>
            ) : null}
            <div className="button-row">
              <button
                className="secondary-button"
                disabled={refreshingArtifact}
                onClick={() => void refreshArtifact()}
                type="button"
              >
                {refreshingArtifact ? "Refreshing..." : "Refresh access"}
              </button>
            </div>
            <section className="subsection">
              <h3>Artifact diagnostics</h3>
              <DiagnosticList diagnostics={artifactDiagnostics} />
            </section>
          </div>
        ) : (
          <p className="muted-text">Choose an artifact from the list.</p>
        )}
      </section>
    </div>
  );
}

export function DiagnosticsRouteShell(): JSX.Element {
  const { apiClient } = useWebUiRuntime();
  const [diagnostics, setDiagnostics] = useState<Diagnostic[]>([]);
  const [observability, setObservability] = useState<ObservabilitySnapshot | null>(null);
  const [subjectType, setSubjectType] = useState("");
  const [severity, setSeverity] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useMessage();
  const [reconcileLimit, setReconcileLimit] = useState(100);
  const [reconcilePending, setReconcilePending] = useState(false);

  const refresh = useCallback(async () => {
    setError("");
    try {
      const [response, snapshot] = await Promise.all([
        apiClient.listDiagnostics(DEFAULT_OWNER, {
          subjectType: subjectType as never,
          severity: severity as never,
          pageSize: 50,
        }),
        apiClient.getObservabilitySnapshot(),
      ]);
      setDiagnostics(response.items);
      setObservability(snapshot);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load diagnostics.");
    }
  }, [apiClient, severity, subjectType]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const reconcileQueue = async () => {
    setError("");
    setMessage("");
    setReconcilePending(true);
    try {
      const response = await apiClient.reconcileAnalysisRunQueue(reconcileLimit);
      setMessage(`Reconciled ${response.reconciled} run tasks`);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to reconcile queue.");
    } finally {
      setReconcilePending(false);
    }
  };

  return (
    <div className="page-grid">
      <section className="surface surface--main">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Refresh
            </button>
          }
          eyebrow="Admin"
          title="Diagnostics"
        />
        <div className="form-grid form-grid--inline">
          <label>
            Subject
            <select value={subjectType} onChange={(event) => setSubjectType(event.target.value)}>
              <option value="">Any</option>
              <option value="media_item">Media item</option>
              <option value="collection">Collection</option>
              <option value="selection">Selection</option>
              <option value="analysis_run">Analysis run</option>
              <option value="artifact">Artifact</option>
              <option value="adapter">Adapter</option>
              <option value="retention">Retention</option>
            </select>
          </label>
          <label>
            Severity
            <select value={severity} onChange={(event) => setSeverity(event.target.value)}>
              <option value="">Any</option>
              <option value="info">Info</option>
              <option value="warning">Warning</option>
              <option value="error">Error</option>
            </select>
          </label>
        </div>
        {error ? <p className="error-text">{error}</p> : null}
        {message ? <p className="success-text">{message}</p> : null}
        <DiagnosticList diagnostics={diagnostics} />
      </section>
      <aside className="side-stack">
        <section className="surface">
          <SectionHeader eyebrow="Observability" title="Queue state" />
          {observability ? (
            <dl className="detail-grid detail-grid--single">
              <div>
                <dt>Queue tasks</dt>
                <dd>{observability.queue_tasks}</dd>
              </div>
              <div>
                <dt>Queue lag</dt>
                <dd>{observability.queue_lag_seconds}s</dd>
              </div>
              <div>
                <dt>Cleanup failures</dt>
                <dd>{observability.cleanup_failures}</dd>
              </div>
              <div>
                <dt>Artifact resolution failures</dt>
                <dd>{observability.artifact_resolution_failures}</dd>
              </div>
              <div>
                <dt>Generated</dt>
                <dd>{formatDate(observability.generated_at)}</dd>
              </div>
            </dl>
          ) : (
            <p className="muted-text">Observability snapshot is not loaded.</p>
          )}
        </section>
        <section className="surface">
          <SectionHeader eyebrow="Lifecycle" title="Queue reconcile" />
          <div className="form-grid">
            <label>
              Limit
              <input
                min={1}
                max={100}
                onChange={(event) => setReconcileLimit(Number(event.target.value) || 1)}
                type="number"
                value={reconcileLimit}
              />
            </label>
            <button disabled={reconcilePending} onClick={() => void reconcileQueue()} type="button">
              {reconcilePending ? "Reconciling..." : "Reconcile queue"}
            </button>
          </div>
        </section>
      </aside>
    </div>
  );
}

export function MediaItemDetailRouteShell(): JSX.Element {
  const { mediaItemId = "" } = useParams();
  const { apiClient } = useWebUiRuntime();
  const [item, setItem] = useState<MediaItem | null>(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useMessage();
  const [removing, setRemoving] = useState(false);

  const refresh = useCallback(async () => {
    setError("");
    try {
      setItem(await apiClient.getMediaItem(DEFAULT_OWNER, mediaItemId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load media item.");
    }
  }, [apiClient, mediaItemId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const removeMediaItem = async () => {
    setError("");
    setMessage("");
    setRemoving(true);
    try {
      const removed = await apiClient.removeMediaItem(DEFAULT_OWNER, mediaItemId);
      setItem(removed);
      setMessage(`Soft-deleted ${removed.display_name}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to remove media item.");
    } finally {
      setRemoving(false);
    }
  };

  return (
    <section className="surface surface--main">
      <SectionHeader
        action={
          <>
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Refresh
            </button>
            <button
              aria-label={item ? `Soft delete ${item.display_name}` : "Soft delete media item"}
              className="secondary-button danger"
              disabled={removing}
              onClick={() => void removeMediaItem()}
              type="button"
            >
              {removing ? "Soft-deleting..." : "Soft delete"}
            </button>
          </>
        }
        eyebrow="Media item"
        title={item?.display_name ?? compactId(mediaItemId)}
      />
      {error ? <p className="error-text">{error}</p> : null}
      {message ? <p className="success-text">{message}</p> : null}
      {item ? (
        <dl className="detail-grid">
          <div>
            <dt>Kind</dt>
            <dd>{item.kind}</dd>
          </div>
          <div>
            <dt>Status</dt>
            <dd>{item.status}</dd>
          </div>
          <div>
            <dt>Retention</dt>
            <dd>{item.retention.state}</dd>
          </div>
          <div>
            <dt>Deleted</dt>
            <dd>{formatDate(item.retention.deleted_at ?? item.deleted_at)}</dd>
          </div>
          <div>
            <dt>Origin</dt>
            <dd>{item.source.origin_type}</dd>
          </div>
          <div>
            <dt>Created</dt>
            <dd>{formatDate(item.created_at)}</dd>
          </div>
          <div>
            <dt>Size</dt>
            <dd>{formatBytes(item.source.size_bytes)}</dd>
          </div>
          <div>
            <dt>Source</dt>
            <dd>{sourceLabel(item)}</dd>
          </div>
        </dl>
      ) : null}
    </section>
  );
}

export function RouteNotFoundShell(): JSX.Element {
  return (
    <section className="surface surface--main">
      <SectionHeader eyebrow="404" title="Surface not found" />
      <Link className="button-link" to="/">
        Open inbox
      </Link>
    </section>
  );
}
