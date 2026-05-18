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

function artifactGroupLabel(kind: string): string {
  switch (kind) {
    case "transcript":
      return "Расшифровка";
    case "summary":
      return "Краткое содержание";
    case "report":
      return "Отчет";
    case "deep_research":
      return "Глубокое исследование";
    case "run_manifest":
      return "План запуска";
    case "run_diagnostics":
    case "diagnostic_bundle":
      return "Проверки";
    case "structured_data":
      return "Данные";
    case "execution_log":
      return "Журнал";
    case "source_manifest":
      return "Список материалов";
    case "preview":
      return "Предпросмотр";
    default:
      return kind.replace(/_/g, " ");
  }
}

function eventLabel(eventType: string): string {
  switch (eventType) {
    case "analysis_run.created":
      return "Запуск создан";
    case "analysis_run.progress":
      return "Прогресс";
    case "analysis_run.completed":
      return "Запуск завершен";
    case "analysis_run.failed":
      return "Ошибка запуска";
    case "diagnostic.recorded":
      return "Проверка записана";
    case "artifact.available":
      return "Результат готов";
    default:
      return "Событие";
  }
}

function runTypeLabel(runType: string): string {
  switch (runType) {
    case "transcription":
      return "Расшифровка";
    case "summary":
      return "Краткое содержание";
    case "report":
      return "Отчет";
    case "deep_research":
      return "Глубокое исследование";
    case "custom":
      return "Свой сценарий";
    default:
      return runType.replace(/_/g, " ");
  }
}

function statusLabel(status: string): string {
  switch (status) {
    case "queued":
      return "В очереди";
    case "running":
      return "В работе";
    case "cancel_requested":
      return "Остановка";
    case "pending":
      return "Готовится";
    case "validating":
      return "Проверяется";
    case "partially_succeeded":
      return "Частично готово";
    case "succeeded":
    case "included":
    case "ready":
    case "available":
    case "active":
      return "Готово";
    case "skipped":
      return "Пропущено";
    case "failed":
      return "Ошибка";
    case "canceled":
      return "Отменено";
    case "expired":
      return "Истекло";
    case "deleted":
    case "soft_deleted":
      return "Удалено";
    case "archived":
      return "В архиве";
    case "quarantined":
      return "На проверке";
    case "warning":
      return "Предупреждение";
    case "info":
      return "Инфо";
    case "error":
      return "Ошибка";
    default:
      return status.replace(/_/g, " ");
  }
}

function kindLabel(kind: string): string {
  switch (kind) {
    case "text":
      return "Текст";
    case "url":
      return "Ссылка";
    case "audio":
    case "voice":
      return "Аудио";
    case "video":
      return "Видео";
    case "image":
    case "photo":
      return "Изображение";
    case "document":
    case "file":
      return "Файл";
    case "object":
    case "upload":
    case "telegram_file":
      return "Файл";
    case "binary":
      return "Данные";
    default:
      return kind.replace(/_/g, " ");
  }
}

function diagnosticSubjectLabel(subjectType: string): string {
  switch (subjectType) {
    case "media_asset":
    case "media_item":
      return "материал";
    case "collection":
      return "группа";
    case "selection_snapshot":
    case "selection":
      return "подборка";
    case "analysis_run":
      return "запуск";
    case "artifact":
      return "результат";
    case "adapter":
    case "channel":
      return "канал";
    case "retention":
      return "хранение";
    default:
      return subjectType.replace(/_/g, " ");
  }
}

function diagnosticCodeLabel(code: string): string {
  switch (code) {
    case "worker_failed":
      return "Сбой обработки";
    case "source_unavailable":
      return "Материал недоступен";
    case "artifact_preview_ready":
      return "Предпросмотр готов";
    case "retention_hold_pending":
      return "Удаление ожидает разрешения";
    case "legacy_source_warning":
      return "Предупреждение по материалу";
    default:
      return "Проверка";
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
    return "Нет данных";
  }
  return new Intl.DateTimeFormat("ru-RU", {
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

function evidenceGateLabel(state: string): string {
  switch (state) {
    case "not_required":
      return "Не требуется";
    case "pending":
      return "Ожидает проверки";
    case "passed":
      return "Пройдена";
    case "failed":
      return "Есть ошибки";
    default:
      return statusLabel(state);
  }
}

function runEventProgressText(event: RunEvent): string {
  if (event.event_type !== "analysis_run.progress" || !isRecord(event.payload)) {
    return "";
  }
  const stage = typeof event.payload.stage === "string" ? event.payload.stage : "";
  const message = typeof event.payload.message === "string" ? event.payload.message : "";
  return [stage ? stageLabel(stage) : "", message].filter(Boolean).join(": ");
}

function stageLabel(stage: string): string {
  switch (stage) {
    case "queued":
      return "Ожидает очереди";
    case "transcribing":
      return "Расшифровка";
    case "summarizing":
      return "Краткое содержание";
    case "reporting":
      return "Отчет";
    case "deep_research":
      return "Глубокое исследование";
    case "artifact_upload":
      return "Сохранение результата";
    default:
      return "Прогресс";
  }
}

function sourceLabel(item: MediaItemSummary): string {
  if (item.source.external_uri) {
    return item.source.external_uri;
  }
  if (item.source.object_key) {
    return "Загруженный файл";
  }
  if (item.source.text_ref) {
    return "Встроенный текст";
  }
  return kindLabel(item.source.origin_type);
}

function selectionSummaryLabel(run: AnalysisRun): string {
  const count = run.selection.items.length;
  if (count === 1) {
    return "1 материал";
  }
  if (count > 1 && count < 5) {
    return `${count} материала`;
  }
  return `${count} материалов`;
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
      setError(err instanceof Error ? err.message : "Не удалось загрузить рабочую область.");
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

export function MediaItemList({
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
    return <p className="muted-text">Материалов пока нет.</p>;
  }

  return (
    <div className="data-list">
      {onToggle && onSelectAll && onClearSelection ? (
        <div className="selection-toolbar" aria-label="Управление подборкой">
          <button className="secondary-button" onClick={onSelectAll} type="button">
            Выбрать все
          </button>
          <button className="secondary-button" disabled={selected.size === 0} onClick={onClearSelection} type="button">
            Очистить
          </button>
        </div>
      ) : null}
      {items.map((item) => (
        <article className="data-row media-row" key={item.media_item_id}>
          {onToggle ? (
            <label className="select-cell">
              <input
                aria-label={`Выбрать ${item.display_name}`}
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
              <dt>Тип</dt>
              <dd>{kindLabel(item.kind)}</dd>
            </div>
            <div>
              <dt>Состояние</dt>
              <dd>
                <span className="status-pill" data-status={item.status}>
                  {statusLabel(item.status)}
                </span>
              </dd>
            </div>
            <div>
              <dt>Добавлен</dt>
              <dd>{formatDate(item.created_at)}</dd>
            </div>
          </dl>
          {onRemove ? (
            <button
              aria-label={`Удалить ${item.display_name}`}
              className="icon-button danger"
              onClick={() => onRemove(item.media_item_id)}
              type="button"
            >
              Удалить
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
          throw new Error("Добавьте текст.");
        }
        draft = {
          kind: "text",
          displayName: displayName.trim() || text.trim().slice(0, 64),
          adapterOrigin: "web",
          source: { origin_type: "text", text: text.trim() },
        };
      } else if (mode === "url") {
        if (url.trim() === "") {
          throw new Error("Добавьте ссылку.");
        }
        draft = {
          kind: "url",
          displayName: displayName.trim() || url.trim(),
          adapterOrigin: "web",
          source: { origin_type: "url", url: url.trim() },
        };
      } else {
        if (!file) {
          throw new Error("Выберите файл.");
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
      setMessage(`Добавлено: ${item.display_name}`);
      setDisplayName("");
      setText("");
      setUrl("");
      setFile(null);
      await onCreated();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось добавить материал.");
    } finally {
      setPending(false);
    }
  };

  return (
    <section className="surface">
      <SectionHeader eyebrow="Добавление" title="Новый материал" />
      <form className="form-grid" onSubmit={(event) => void submit(event)}>
        <div className="segmented" role="group" aria-label="Тип материала">
          {(["text", "url", "file"] as const).map((entry) => (
            <button
              aria-pressed={mode === entry}
              className="segment-button"
              key={entry}
              onClick={() => setMode(entry)}
              type="button"
            >
              {entry === "file" ? "Файл" : entry === "url" ? "Ссылка" : "Текст"}
            </button>
          ))}
        </div>
        <label>
          Название
          <input value={displayName} onChange={(event) => setDisplayName(event.target.value)} />
        </label>
        {mode === "text" ? (
          <label>
            Текст
            <textarea rows={5} value={text} onChange={(event) => setText(event.target.value)} />
          </label>
        ) : null}
        {mode === "url" ? (
          <label>
            Ссылка
            <input value={url} onChange={(event) => setUrl(event.target.value)} />
          </label>
        ) : null}
        {mode === "file" ? (
          <label>
            Файл
            <input
              onChange={(event) => setFile(event.target.files?.[0] ?? null)}
              type="file"
            />
          </label>
        ) : null}
        <button disabled={pending} type="submit">
          {pending ? "Добавляем..." : "Добавить"}
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
        name: collectionName.trim() || `Подборка ${new Date().toISOString().slice(0, 10)}`,
        items: Array.from(selected),
      });
      setSelected(new Set());
      setCollectionName("");
      setActionMessage(`Создана группа: ${collection.name}`);
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Не удалось создать группу.");
    } finally {
      setActionPending(false);
    }
  };

  const addToCollection = async () => {
    const collection = collections.find((candidate) => candidate.collection_id === targetCollectionId);
    if (!collection) {
      setActionError("Выберите группу.");
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
      setActionMessage(`Обновлена группа: ${collection.name}`);
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Не удалось обновить группу.");
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
      setActionMessage(`Удалено: ${removed.display_name}`);
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Не удалось удалить материал.");
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
              Обновить
            </button>
          }
          eyebrow="Материалы"
          title="Все материалы"
        />
        {loading ? <p className="muted-text">Загружаем материалы...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        {inbox ? (
          <div className="metric-strip">
            <div>
              <strong>{inbox.items.length}</strong>
              <span>в списке</span>
            </div>
            <div>
              <strong>{mediaItems.length}</strong>
              <span>материалов</span>
            </div>
            <div>
              <strong>{collections.length}</strong>
              <span>групп</span>
            </div>
            <div>
              <strong>{activeRuns.length}</strong>
              <span>в работе</span>
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
          <SectionHeader eyebrow="Подборка" title={`Выбрано: ${selected.size}`} />
          <div className="form-grid">
            <label>
              Новая группа
              <input value={collectionName} onChange={(event) => setCollectionName(event.target.value)} />
            </label>
            <button disabled={actionPending || selected.size === 0} onClick={() => void createCollection()} type="button">
              Создать группу
            </button>
            <label>
              Существующая группа
              <select value={targetCollectionId} onChange={(event) => setTargetCollectionId(event.target.value)}>
                <option value="">Выберите группу</option>
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
              Добавить выбранное
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
        name: name.trim() || `Группа ${userCollections.length + 1}`,
        items: selectedMediaId ? [selectedMediaId] : [],
      });
      setMessage(`Создана группа: ${collection.name}`);
      setName("");
      setSelectedMediaId("");
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Не удалось создать группу.");
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
      setActionError(err instanceof Error ? err.message : "Не удалось переименовать группу.");
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
      setActionError(err instanceof Error ? err.message : "Не удалось убрать материал.");
    }
  };

  const addItem = async (collection: Collection) => {
    const mediaItemId = addTargets[collection.collection_id]!;
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
      setActionError(err instanceof Error ? err.message : "Не удалось добавить материал.");
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
      setActionError(err instanceof Error ? err.message : "Не удалось обновить группу.");
    }
  };

  return (
    <div className="page-grid">
      <section className="surface">
        <SectionHeader eyebrow="Группы" title="Управление группами" />
        <div className="form-grid form-grid--inline">
          <label>
            Название
            <input value={name} onChange={(event) => setName(event.target.value)} />
          </label>
          <label>
            Первый материал
            <select value={selectedMediaId} onChange={(event) => setSelectedMediaId(event.target.value)}>
              <option value="">Без материала</option>
              {mediaItems.map((item) => (
                <option key={item.media_item_id} value={item.media_item_id}>
                  {item.display_name}
                </option>
              ))}
            </select>
          </label>
          <button onClick={() => void create()} type="button">
            Создать
          </button>
        </div>
        {message ? <p className="success-text">{message}</p> : null}
        {actionError ? <p className="error-text">{actionError}</p> : null}
      </section>

      <section className="surface surface--main">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Обновить
            </button>
          }
          eyebrow="Группы"
          title="Список групп"
        />
        {loading ? <p className="muted-text">Загружаем группы...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        <div className="collection-grid">
          {userCollections.map((collection) => (
            <article className="surface surface--flat" key={collection.collection_id}>
              <div className="collection-title">
                <input
                  aria-label={`Переименовать ${collection.name}`}
                  defaultValue={collection.name}
                  onBlur={(event) => void rename(collection, event.target.value)}
                />
                <span className="status-pill" data-status={collection.status}>
                  {statusLabel(collection.status)}
                </span>
              </div>
              <p className="muted-text">
                Материалов: {collection.items.length}
              </p>
              <div className="mini-list">
                {collection.items.map((item) => (
                  <div className="mini-row" key={item.media_item_id}>
                    <span>{item.media_item?.display_name ?? "Материал"}</span>
                    <button
                      className="text-button danger"
                      onClick={() => void removeItem(collection, item.media_item_id)}
                      type="button"
                    >
                      Убрать
                    </button>
                  </div>
                ))}
              </div>
              <div className="form-grid form-grid--compact">
                <label>
                  Добавить материал
                  <select
                    value={addTargets[collection.collection_id] ?? ""}
                    onChange={(event) =>
                      setAddTargets((current) => ({
                        ...current,
                        [collection.collection_id]: event.target.value,
                      }))
                    }
                  >
                    <option value="">Выберите материал</option>
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
                  Добавить
                </button>
              </div>
              <div className="button-row">
                <button
                  className="secondary-button"
                  onClick={() => void setArchiveState(collection, collection.status === "archived" ? "active" : "archived")}
                  type="button"
                >
                  {collection.status === "archived" ? "Вернуть" : "В архив"}
                </button>
                <Link className="button-link" to={`/runs?collection=${collection.collection_id}`}>
                  Запустить
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
      setMessage("Запуск добавлен в очередь");
      await refresh();
    } catch (err) {
      setRunError(err instanceof Error ? err.message : "Не удалось запустить обработку.");
    } finally {
      setPending(false);
    }
  };

  return (
    <div className="page-grid page-grid--builder">
      <section className="surface surface--main">
        <SectionHeader eyebrow="Подборка" title="Запустить обработку" />
        <div className="form-grid form-grid--inline">
          <label>
            Группа
            <select value={sourceCollectionId} onChange={(event) => setSourceCollectionId(event.target.value)}>
              <option value="">Выбрать вручную</option>
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
            Что сделать
            <select value={runType} onChange={(event) => setRunType(event.target.value as RunType)}>
              <option value="transcription">Расшифровать</option>
              <option value="summary">Краткое содержание</option>
              <option value="report">Отчет</option>
              <option value="deep_research">Глубокое исследование</option>
              <option value="custom">Свой сценарий</option>
            </select>
          </label>
          <label>
            Параметры
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
            {pending ? "Запускаем..." : `Запустить: ${selected.size}`}
          </button>
          {message ? <p className="success-text">{message}</p> : null}
          {runError ? <p className="error-text">{runError}</p> : null}
        </div>
      </section>

      <section className="surface">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Обновить
            </button>
          }
          eyebrow="Запуски"
          title="Последние запуски"
        />
        {loading ? <p className="muted-text">Загружаем запуски...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        <RunList runs={runs} />
      </section>
      <section className="surface">
        <SectionHeader eyebrow="План" title={`Материалов: ${selected.size}`} />
        <dl className="detail-grid detail-grid--single">
          <div>
            <dt>Основа</dt>
            <dd>
              {sourceCollectionId
                ? collections.find((collection) => collection.collection_id === sourceCollectionId)?.name ?? "Выбранная группа"
                : "Выбрано вручную"}
            </dd>
          </div>
          <div>
            <dt>Действие</dt>
            <dd>{runTypeLabel(runType)}</dd>
          </div>
          <div>
            <dt>Готовность</dt>
            <dd>Подборка зафиксирована</dd>
          </div>
        </dl>
        {selectedItems.length > 0 ? (
          <div className="mini-list">
            {selectedItems.map((item, index) => (
              <div className="mini-row" key={item.media_item_id}>
                <span>
                  #{index + 1} {item.display_name}
                </span>
                <span className="muted-text">{kindLabel(item.kind)}</span>
              </div>
            ))}
          </div>
        ) : (
          <p className="muted-text">Выберите материалы или группу.</p>
        )}
        {lastPlan ? (
          <dl className="detail-grid detail-grid--single">
            <div>
              <dt>Последняя подборка</dt>
              <dd>Зафиксирована</dd>
            </div>
            <div>
              <dt>Последний запуск</dt>
              <dd>
                <Link className="text-link" to={`/runs/${lastPlan.runId}`}>
                  Открыть запуск
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
    return <p className="muted-text">Запусков пока нет.</p>;
  }
  return (
    <div className="data-list">
      {runs.map((run) => (
        <article className="data-row" key={run.analysis_run_id}>
          <div className="row-main">
            <Link className="text-link" to={`/runs/${run.analysis_run_id}`}>
              {runTypeLabel(run.run_type)}
            </Link>
            <p className="muted-text">{formatDate(run.created_at)}</p>
          </div>
          <dl className="row-meta">
            <div>
              <dt>Состояние</dt>
              <dd>
                <span className="status-pill" data-status={run.status}>
                  {statusLabel(run.status)}
                </span>
              </dd>
            </div>
            <div>
              <dt>Результаты</dt>
              <dd>{run.artifact_count ?? 0}</dd>
            </div>
            <div>
              <dt>Создан</dt>
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
        { subject_type: "selection_snapshot", subject_id: runResponse.selection_snapshot_id ?? runResponse.selection_id },
        ...runResponse.selection.items.flatMap((item) => [
          { subject_type: "media_asset", subject_id: item.media_item_id },
          { subject_type: "stored_object", subject_id: item.source_snapshot.source_id },
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
      setError(err instanceof Error ? err.message : "Не удалось загрузить запуск.");
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
      setMessage("Остановка запрошена");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось остановить запуск.");
    }
  };

  const retry = async () => {
    setMessage("");
    try {
      await apiClient.retryAnalysisRun(DEFAULT_OWNER, analysisRunId);
      setMessage("Повторный запуск добавлен в очередь");
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось повторить запуск.");
    }
  };

  const manifest = parseRunManifest([...(run?.artifacts ?? []), ...artifacts]);

  return (
    <div className="page-grid page-grid--detail">
      <section className="surface surface--main">
        <SectionHeader
          action={
            <button className="secondary-button" onClick={() => void refresh()} type="button">
              Обновить
            </button>
          }
          eyebrow="Запуск"
          title={run ? runTypeLabel(run.run_type) : "Загрузка"}
        />
        {loading ? <p className="muted-text">Загружаем запуск...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        {message ? <p className="success-text">{message}</p> : null}
        {run ? (
          <>
            <dl className="detail-grid">
              <div>
                <dt>Состояние</dt>
                <dd>
                  <span className="status-pill" data-status={run.status}>
                    {statusLabel(run.status)}
                  </span>
                </dd>
              </div>
              <div>
                <dt>Подборка</dt>
                <dd>{selectionSummaryLabel(run)}</dd>
              </div>
              <div>
                <dt>Версия</dt>
                <dd>{run.version}</dd>
              </div>
              <div>
                <dt>Проверка готовности</dt>
                <dd>{evidenceGateLabel(run.evidence_gate_state)}</dd>
              </div>
              <div>
                <dt>Создан</dt>
                <dd>{formatDate(run.created_at)}</dd>
              </div>
              <div>
                <dt>Завершен</dt>
                <dd>{formatDate(run.completed_at)}</dd>
              </div>
            </dl>
            <div className="button-row">
              <button disabled={!ACTIVE_RUN_STATUSES.has(run.status)} onClick={() => void cancel()} type="button">
                Остановить
              </button>
              <button className="secondary-button" onClick={() => void retry()} type="button">
                Повторить
              </button>
            </div>
            <section className="subsection">
              <h3>Подборка</h3>
              <div className="mini-list">
                {run.selection.items.map((item) => (
                  <div className="mini-row" key={`${item.media_item_id}-${item.position}`}>
                    <span>
                      #{item.position + 1} {item.display_name}
                    </span>
                    <span className="muted-text">{kindLabel(item.kind)}</span>
                  </div>
                ))}
              </div>
            </section>
            <section className="subsection">
              <h3>Итоги по материалам</h3>
              <RunOutcomeList run={run} manifest={manifest} diagnostics={diagnostics} />
            </section>
            <section className="subsection">
              <h3>Проверки по материалам</h3>
              <SourceDiagnosticsList run={run} diagnostics={diagnostics} />
            </section>
          </>
        ) : null}
      </section>

      <aside className="side-stack">
        <section className="surface">
          <SectionHeader eyebrow="События" title="Ход работы" />
          <EventList events={events} />
        </section>
        <section className="surface">
          <SectionHeader eyebrow="Результаты" title="Файлы и отчеты" />
          <ArtifactList artifacts={artifacts} />
        </section>
        <section className="surface">
          <SectionHeader eyebrow="Проверки" title="Ошибки и предупреждения" />
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
              <span>включено</span>
            </div>
            <div>
              <strong>{manifest.summary.skipped_count ?? 0}</strong>
              <span>пропущено</span>
            </div>
            <div>
              <strong>{manifest.summary.failed_count ?? 0}</strong>
              <span>ошибок</span>
            </div>
          </div>
        ) : null}
        <div className="data-list">
          {manifest.items.map((item) => {
            return (
              <article className="data-row outcome-row" key={`${item.media_item_id}-${item.position}`}>
                <div className="row-main">
                  <strong>
                    #{item.position + 1} Материал
                  </strong>
                  <p className="muted-text">{item.selection_item_id ? "Выбран в подборке" : "Элемент подборки"}</p>
                </div>
                <dl className="row-meta">
                  <div>
                    <dt>Итог</dt>
                    <dd>
                      <span className="status-pill" data-status={item.outcome}>
                        {statusLabel(item.outcome)}
                      </span>
                    </dd>
                  </div>
                  <div>
                    <dt>Результаты</dt>
                    <dd>{item.artifact_kinds?.map((kind) => artifactGroupLabel(kind as ArtifactSummary["kind"])).join(", ") || "Нет"}</dd>
                  </div>
                  <div>
                    <dt>Проверки</dt>
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
              <p className="muted-text">Выбран в подборке</p>
            </div>
            <dl className="row-meta">
              <div>
                <dt>Состояние</dt>
                <dd>{statusLabel(item.status_at_selection)}</dd>
              </div>
              <div>
                <dt>Откуда</dt>
                <dd>{kindLabel(item.source_snapshot.origin_type)}</dd>
              </div>
              <div>
                <dt>Проверки</dt>
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
    return <p className="muted-text">Проверок по материалам нет.</p>;
  }

  return (
    <div className="data-list">
      {entries
        .filter((entry) => entry.diagnostics.length > 0)
        .map((entry) => (
          <article className="data-row source-diagnostic-row" key={entry.item.media_item_id}>
            <div className="row-main">
              <strong>{entry.item.display_name}</strong>
              <p className="muted-text">{kindLabel(entry.item.source_snapshot.origin_type)}</p>
            </div>
            <DiagnosticList diagnostics={entry.diagnostics} />
          </article>
        ))}
    </div>
  );
}

function EventList({ events }: { events: RunEvent[] }): JSX.Element {
  if (events.length === 0) {
    return <p className="muted-text">Событий пока нет.</p>;
  }
  return (
    <div className="timeline-list">
      {events.map((event) => (
        <article className="timeline-entry" key={event.event_id}>
          <strong>{eventLabel(event.event_type)}</strong>
          <span className="muted-text">версия {event.version}</span>
          <span className="muted-text">{formatDate(event.emitted_at)}</span>
          {runEventProgressText(event) ? (
            <span className="muted-text">{runEventProgressText(event)}</span>
          ) : null}
          {event.status ? (
            <span className="status-pill" data-status={event.status}>
              {statusLabel(event.status)}
            </span>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function ArtifactList({ artifacts }: { artifacts: ArtifactSummary[] }): JSX.Element {
  if (artifacts.length === 0) {
    return <p className="muted-text">Результатов пока нет.</p>;
  }
  return (
    <div className="data-list">
      {artifacts.map((artifact) => (
        <article className="data-row" key={artifact.artifact_id}>
          <div className="row-main">
            <Link className="text-link" to={`/artifacts/${artifact.artifact_id}`}>
              {artifactGroupLabel(artifact.kind)}
            </Link>
            <p className="muted-text">{artifact.content_type}</p>
          </div>
          <dl className="row-meta">
            <div>
              <dt>Состояние</dt>
              <dd>{statusLabel(artifact.status)}</dd>
            </div>
            <div>
              <dt>Размер</dt>
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
    return <p className="muted-text">Результатов пока нет.</p>;
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
                Открыть запуск
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
    return <p className="muted-text">Проверок пока нет.</p>;
  }
  return (
    <div className="data-list">
      {diagnostics.map((diagnostic) => (
        <article className="data-row diagnostic-row" key={diagnostic.diagnostic_id}>
          <div className="row-main">
            <strong>{diagnosticCodeLabel(diagnostic.code)}</strong>
            <p className="muted-text">{diagnostic.message}</p>
            <p className="muted-text">
              {diagnosticSubjectLabel(diagnosticSubject(diagnostic).subject_type)}
            </p>
          </div>
          <span className="status-pill" data-status={diagnostic.severity}>
            {statusLabel(diagnostic.severity)}
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
      setError(err instanceof Error ? err.message : "Не удалось загрузить результаты.");
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
      setMessage("Ссылка обновлена");
      const artifactsResponse = await apiClient.listArtifacts(DEFAULT_OWNER, { pageSize: 50 });
      setArtifacts(artifactsResponse.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось обновить ссылку.");
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
              Обновить
            </button>
          }
          eyebrow="Результаты"
          title="Файлы и отчеты"
        />
        {error ? <p className="error-text">{error}</p> : null}
        {message ? <p className="success-text">{message}</p> : null}
        <GroupedArtifactList artifacts={artifacts} />
      </section>
      <section className="surface">
        <SectionHeader eyebrow="Просмотр" title={artifact ? artifactGroupLabel(artifact.kind) : "Выберите результат"} />
        {artifact ? (
          <div className="artifact-preview">
            <dl className="detail-grid">
              <div>
                <dt>Состояние</dt>
                <dd>{statusLabel(artifact.status)}</dd>
              </div>
              <div>
                <dt>Тип содержимого</dt>
                <dd>{artifact.content_type}</dd>
              </div>
              <div>
                <dt>Размер</dt>
                <dd>{formatBytes(artifact.size_bytes)}</dd>
              </div>
              <div>
                <dt>Запуск</dt>
                <dd>
                  <Link className="text-link" to={`/runs/${artifact.analysis_run_id}`}>
                    Открыть запуск
                  </Link>
                </dd>
              </div>
              <div>
                <dt>Формат</dt>
                <dd>{artifact.preview?.format ?? previewFormat(artifact)}</dd>
              </div>
            </dl>
            {previewFormat(artifact) !== "none" && previewText(artifact) ? (
              <pre data-format={previewFormat(artifact)}>{previewText(artifact)}</pre>
            ) : (
              <p className="muted-text">Предпросмотр недоступен.</p>
            )}
            {(artifact.download?.available ?? Boolean(artifact.download?.url)) && artifact.download?.url ? (
              <a className="button-link" href={artifact.download.url} rel="noreferrer" target="_blank">
                Открыть результат
              </a>
            ) : null}
            <div className="button-row">
              <button
                className="secondary-button"
                disabled={refreshingArtifact}
                onClick={() => void refreshArtifact()}
                type="button"
              >
                {refreshingArtifact ? "Обновляем..." : "Обновить ссылку"}
              </button>
            </div>
            <section className="subsection">
              <h3>Проверки результата</h3>
              <DiagnosticList diagnostics={artifactDiagnostics} />
            </section>
          </div>
        ) : (
          <p className="muted-text">Выберите результат из списка.</p>
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
      setError(err instanceof Error ? err.message : "Не удалось загрузить проверки.");
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
      setMessage(`Синхронизировано: ${response.reconciled}`);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось синхронизировать очередь.");
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
              Обновить
            </button>
          }
          eyebrow="Администрирование"
          title="Проверки"
        />
        <div className="form-grid form-grid--inline">
          <label>
            Объект
            <select value={subjectType} onChange={(event) => setSubjectType(event.target.value)}>
              <option value="">Любой</option>
              <option value="media_asset">Материал</option>
              <option value="stored_object">Файл</option>
              <option value="collection">Группа</option>
              <option value="selection_snapshot">Подборка</option>
              <option value="analysis_run">Запуск</option>
              <option value="artifact">Результат</option>
              <option value="channel">Канал</option>
              <option value="retention">Хранение</option>
            </select>
          </label>
          <label>
            Уровень
            <select value={severity} onChange={(event) => setSeverity(event.target.value)}>
              <option value="">Любой</option>
              <option value="info">Инфо</option>
              <option value="warning">Предупреждение</option>
              <option value="error">Ошибка</option>
            </select>
          </label>
        </div>
        {error ? <p className="error-text">{error}</p> : null}
        {message ? <p className="success-text">{message}</p> : null}
        <DiagnosticList diagnostics={diagnostics} />
      </section>
      <aside className="side-stack">
        <section className="surface">
          <SectionHeader eyebrow="Наблюдение" title="Состояние очереди" />
          {observability ? (
            <dl className="detail-grid detail-grid--single">
              <div>
                <dt>Задач в очереди</dt>
                <dd>{observability.queue_tasks}</dd>
              </div>
              <div>
                <dt>Задержка</dt>
                <dd>{observability.queue_lag_seconds}s</dd>
              </div>
              <div>
                <dt>Ошибки очистки</dt>
                <dd>{observability.cleanup_failures}</dd>
              </div>
              <div>
                <dt>Ошибки результатов</dt>
                <dd>{observability.artifact_resolution_failures}</dd>
              </div>
              <div>
                <dt>Обновлено</dt>
                <dd>{formatDate(observability.generated_at)}</dd>
              </div>
            </dl>
          ) : (
            <p className="muted-text">Снимок состояния не загружен.</p>
          )}
        </section>
        <section className="surface">
          <SectionHeader eyebrow="Обслуживание" title="Синхронизация очереди" />
          <div className="form-grid">
            <label>
              Лимит
              <input
                min={1}
                max={100}
                onChange={(event) => setReconcileLimit(Number(event.target.value) || 1)}
                type="number"
                value={reconcileLimit}
              />
            </label>
            <button disabled={reconcilePending} onClick={() => void reconcileQueue()} type="button">
              {reconcilePending ? "Синхронизируем..." : "Синхронизировать"}
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
      setError(err instanceof Error ? err.message : "Не удалось загрузить материал.");
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
      setMessage(`Удалено: ${removed.display_name}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось удалить материал.");
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
              Обновить
            </button>
            <button
              aria-label={item ? `Удалить ${item.display_name}` : "Удалить материал"}
              className="secondary-button danger"
              disabled={removing}
              onClick={() => void removeMediaItem()}
              type="button"
            >
              {removing ? "Удаляем..." : "Удалить"}
            </button>
          </>
        }
        eyebrow="Материал"
        title={item?.display_name ?? "Материал"}
      />
      {error ? <p className="error-text">{error}</p> : null}
      {message ? <p className="success-text">{message}</p> : null}
      {item ? (
        <dl className="detail-grid">
          <div>
            <dt>Тип</dt>
            <dd>{kindLabel(item.kind)}</dd>
          </div>
          <div>
            <dt>Состояние</dt>
            <dd>{statusLabel(item.status)}</dd>
          </div>
          <div>
            <dt>Хранение</dt>
            <dd>{statusLabel(item.retention.state)}</dd>
          </div>
          <div>
            <dt>Удален</dt>
            <dd>{formatDate(item.retention.deleted_at ?? item.deleted_at)}</dd>
          </div>
          <div>
            <dt>Тип источника</dt>
            <dd>{kindLabel(item.source.origin_type)}</dd>
          </div>
          <div>
            <dt>Создан</dt>
            <dd>{formatDate(item.created_at)}</dd>
          </div>
          <div>
            <dt>Размер</dt>
            <dd>{formatBytes(item.source.size_bytes)}</dd>
          </div>
          <div>
            <dt>Откуда</dt>
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
      <SectionHeader eyebrow="404" title="Страница не найдена" />
      <Link className="button-link" to="/">
        К материалам
      </Link>
    </section>
  );
}
