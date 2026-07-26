import { useCallback, useEffect, useMemo, useState, type FormEvent, type ReactNode } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { RECONCILE_STATE_MARKER, requiresRestReconciliation } from "../../lib/api/client";
import type {
  AddMediaAssetInput,
  AnalysisRun,
  AnalysisRunSummary,
  Artifact,
  ArtifactSummary,
  Collection,
  Diagnostic,
  ExportJob,
  ExportOperation,
  ExportVariant,
  MediaAsset,
  MediaAssetOrigin,
  MediaAssetSummary,
  ObservabilitySnapshot,
  RunEvent,
  RunType,
} from "../../lib/api/types";
import { useWebUiRuntime } from "../../app/runtime-context";

const ACTIVE_RUN_STATUSES = new Set(["queued", "running", "cancel_requested"]);
const ACTIVE_EXPORT_STATUSES = new Set(["queued", "claimed", "running", "cancel_requested"]);
const RETRYABLE_EXPORT_STATUSES = new Set(["failed", "canceled"]);
const EXPORT_JOB_POLL_INTERVAL_MS = 2_000;
const AUDIO_BITRATES = [64, 96, 128, 192, 256] as const;
const VIDEO_QUALITIES = ["360p", "480p", "720p", "1080p"] as const;

interface DiagnosticSubject {
  subject_type: string;
  subject_id: string;
}

interface RunManifestItemOutcome {
  selection_snapshot_item_id?: string;
  media_asset_id: string;
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
  return diagnostic.subject ?? {
    subject_type: "analysis_run",
    subject_id: "",
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

const INTERNAL_RESULT_ARTIFACT_KINDS = new Set<string>([
  "run_manifest",
  "run_diagnostics",
  "diagnostic_bundle",
  "execution_log",
]);

function isUserVisibleArtifactKind(kind: string): boolean {
  return !INTERNAL_RESULT_ARTIFACT_KINDS.has(kind);
}

function userVisibleArtifacts<TArtifact extends { kind: string }>(artifacts: TArtifact[]): TArtifact[] {
  return artifacts.filter((artifact) => isUserVisibleArtifactKind(artifact.kind));
}

function userVisibleArtifactLabels(kinds?: string[]): string {
  const labels = (kinds ?? []).filter(isUserVisibleArtifactKind).map(artifactGroupLabel);
  return labels.length ? labels.join(", ") : "Нет";
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
    case "structured_data":
      return "Данные";
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
    case "claimed":
      return "Задача принята";
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
    case "collection":
      return "группа";
    case "selection_snapshot":
      return "подборка";
    case "analysis_run":
      return "запуск";
    case "artifact":
      return "результат";
    case "channel_account":
    case "channel_surface":
      return "канал";
    default:
      return subjectType.replace(/_/g, " ");
  }
}

function diagnosticCodeLabel(code: string): string {
  switch (code) {
    case "worker_failed":
      return "Сбой обработки";
    case "origin_unavailable":
      return "Материал недоступен";
    case "artifact_preview_ready":
      return "Предпросмотр готов";
    case "retention_denied":
      return "Удаление отклонено";
    case "origin_warning":
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

function originLabel(origin: MediaAssetOrigin): string {
  if (origin.url || origin.origin_type === "url") {
    return origin.url ?? origin.origin_ref ?? "Ссылка";
  }
  if (origin.object_ref || origin.origin_type === "upload" || origin.origin_type === "telegram_file") {
    return "Загруженный файл";
  }
  if (origin.text || origin.origin_type === "text") {
    return "Встроенный текст";
  }
  return kindLabel(origin.origin_type);
}

function originObjectRef(origin: MediaAssetOrigin): string {
  return origin.stored_object_id ?? origin.object_ref ?? origin.origin_ref ?? "";
}

function isYouTubeAsset(item: MediaAssetSummary): boolean {
  const reference = item.origin.url ?? item.origin.origin_ref ?? "";
  try {
    const host = new URL(reference).hostname.toLowerCase();
    return host === "youtu.be" || host.endsWith("youtube.com");
  } catch {
    return false;
  }
}

function isYouTubeProviderMetadata(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value)) {
    return false;
  }
  const provider = value.provider;
  return typeof provider !== "string" || provider.trim().toLowerCase() === "youtube";
}

function preferredYoutubeMetadata(item: MediaAssetSummary, key: "provider_metadata" | "enrichment"): Record<string, unknown> | null {
  const metadata = isRecord(item.metadata) ? item.metadata : null;
  const topLevel = item[key];
  if (isYouTubeProviderMetadata(topLevel)) {
    return topLevel;
  }
  const nested = metadata?.[key];
  return isYouTubeProviderMetadata(nested) ? nested : null;
}

function providerString(metadata: Record<string, unknown> | null, key: string): string | null {
  const candidate = metadata?.[key];
  if (typeof candidate === "string" && candidate.trim()) {
    return candidate.trim();
  }
  return null;
}

function providerDurationSeconds(metadata: Record<string, unknown> | null): number | null {
  const candidate = metadata?.duration_seconds;
  if (typeof candidate === "number" && Number.isFinite(candidate) && candidate > 0) {
    return candidate;
  }
  return null;
}

function formatMediaDuration(seconds: number): string {
  const totalSeconds = Math.round(seconds);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainingSeconds = totalSeconds % 60;
  const twoDigits = (value: number) => String(value).padStart(2, "0");
  return hours > 0 ? `${hours}:${twoDigits(minutes)}:${twoDigits(remainingSeconds)}` : `${minutes}:${twoDigits(remainingSeconds)}`;
}

function youtubeMetadata(item: MediaAssetSummary): { title: string | null; status: string | null; duration: string | null } | null {
  if (!isYouTubeAsset(item)) {
    return null;
  }
  const provider = preferredYoutubeMetadata(item, "provider_metadata");
  const enrichment = preferredYoutubeMetadata(item, "enrichment");
  const durationSeconds = providerDurationSeconds(provider) ?? providerDurationSeconds(enrichment);
  return {
    title: providerString(provider, "title") ?? providerString(enrichment, "title"),
    status: (providerString(enrichment, "status") ?? providerString(provider, "status"))?.toLowerCase() ?? null,
    duration: durationSeconds === null ? null : formatMediaDuration(durationSeconds),
  };
}

function mediaAssetDisplayName(item: MediaAssetSummary): string {
  const metadata = youtubeMetadata(item);
  if (metadata?.title) {
    return metadata.title;
  }
  if (metadata && /^https?:\/\//i.test(item.display_name.trim())) {
    return "Видео YouTube";
  }
  return item.display_name;
}

function assetOriginLabel(item: MediaAssetSummary): string {
  const metadata = youtubeMetadata(item);
  if (!metadata) {
    return originLabel(item.origin);
  }
  if (metadata.title) {
    return ["YouTube", metadata.duration].filter(Boolean).join(" · ");
  }
  if (metadata.status === "failed") {
    return "Метаданные YouTube недоступны";
  }
  if (metadata.status === "succeeded") {
    return metadata.duration ? `YouTube · ${metadata.duration}` : "Метаданные YouTube готовы";
  }
  return "Метаданные YouTube загружаются";
}

function isUploadedVideoAsset(item: MediaAssetSummary): boolean {
  return item.kind === "video" && !isYouTubeAsset(item) && item.origin.origin_type !== "url";
}

function exportActionLabel(item: MediaAssetSummary): string | null {
  if (isYouTubeAsset(item)) {
    return "Скачать";
  }
  if (isUploadedVideoAsset(item)) {
    return "В аудио";
  }
  return null;
}

function exportOperationLabel(operation: ExportOperation): string {
  switch (operation) {
    case "youtube_audio":
      return "YouTube: аудио";
    case "youtube_video":
      return "YouTube: видео";
    case "video_to_audio":
      return "Видео в аудио";
  }
}

function exportVariantLabel(variant: ExportVariant): string {
  if (variant.video_quality) {
    return variant.video_quality;
  }
  return variant.audio_bitrate_kbps ? `${variant.audio_bitrate_kbps} кбит/с` : "Стандартное качество";
}

function exportProgressLabel(job: ExportJob): string {
  const stageLabels: Record<string, string> = {
    queued: "Ожидает очереди",
    resolving_source: "Подготавливаем источник",
    downloading: "Получаем файл",
    converting: "Конвертируем",
    publishing: "Сохраняем файл",
  };
  const detail = job.progress.message ?? stageLabels[job.progress.stage] ?? "Выполняется";
  return job.progress.percent !== undefined ? `${Math.round(job.progress.percent)}% · ${detail}` : detail;
}

function reconcileExportJobs(current: ExportJob[], updates: ExportJob[]): ExportJob[] {
  const currentIds = new Set(current.map((job) => job.export_job_id));
  const updatesById = new Map(updates.map((job) => [job.export_job_id, job]));
  const added = updates.filter((job) => !currentIds.has(job.export_job_id));
  const reconciled = current.map((job) => {
    const update = updatesById.get(job.export_job_id);
    if (!update || update.version < job.version) {
      return job;
    }
    return update;
  });
  return [...added, ...reconciled];
}

function selectionSummaryLabel(run: AnalysisRun): string {
  const count = run.selection_snapshot.items.length;
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
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const [mediaAssets, setMediaAssets] = useState<MediaAssetSummary[]>([]);
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
        apiClient.listMediaAssets(channelAccountId, { pageSize: 50 }),
        apiClient.getInboxCollection(channelAccountId),
        apiClient.listCollections(channelAccountId, { pageSize: 50 }),
        apiClient.listAnalysisRuns(channelAccountId, { pageSize: 25 }),
      ]);
      setMediaAssets(mediaResponse.items);
      setInbox(inboxResponse);
      setCollections(collectionsResponse.items);
      setRuns(runsResponse.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось загрузить рабочую область.");
    } finally {
      setLoading(false);
    }
  }, [apiClient, channelAccountId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return {
    collections,
    error,
    inbox,
    loading,
    mediaAssets,
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

export function MediaAssetList({
  items,
  selected,
  onToggle,
  onSelectAll,
  onClearSelection,
  onRemove,
}: {
  items: MediaAssetSummary[];
  selected: Set<string>;
  onToggle?: (mediaAssetId: string) => void;
  onSelectAll?: () => void;
  onClearSelection?: () => void;
  onRemove?: (mediaAssetId: string) => void;
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
        <article className="data-row media-row" key={item.media_asset_id}>
          {onToggle ? (
            <label className="select-cell">
              <input
                aria-label={`Выбрать ${item.display_name}`}
                checked={selected.has(item.media_asset_id)}
                onChange={() => onToggle(item.media_asset_id)}
                type="checkbox"
              />
            </label>
          ) : null}
          <div className="row-main">
            <Link className="text-link" to={`/inbox/${item.media_asset_id}`}>
              {mediaAssetDisplayName(item)}
            </Link>
            <p className="muted-text">{assetOriginLabel(item)}</p>
            {exportActionLabel(item) ? (
              <Link className="text-button" to={`/exports?media_asset_id=${encodeURIComponent(item.media_asset_id)}`}>
                {exportActionLabel(item)}
              </Link>
            ) : null}
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
              onClick={() => onRemove(item.media_asset_id)}
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
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
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
      let item: MediaAsset;
      if (mode === "text") {
        if (text.trim() === "") {
          throw new Error("Добавьте текст.");
        }
        const input: AddMediaAssetInput = {
          kind: "text",
          displayName: displayName.trim() || text.trim().slice(0, 64),
          origin: { origin_type: "text", text: text.trim(), origin_ref: text.trim().slice(0, 64) },
        };
        item = await apiClient.addMediaAsset(channelAccountId, input);
      } else if (mode === "url") {
        if (url.trim() === "") {
          throw new Error("Добавьте ссылку.");
        }
        const input: AddMediaAssetInput = {
          kind: "url",
          displayName: displayName.trim() || url.trim(),
          origin: { origin_type: "url", url: url.trim(), origin_ref: url.trim() },
        };
        item = await apiClient.addMediaAsset(channelAccountId, input);
      } else {
        if (!file) {
          throw new Error("Выберите файл.");
        }
        const kind = file.type.startsWith("audio/")
          ? "audio"
          : file.type.startsWith("video/")
            ? "video"
            : file.type.startsWith("image/")
              ? "image"
              : "document";
        item = await apiClient.uploadMediaAsset(
          channelAccountId,
          file,
          kind,
          displayName.trim() || file.name,
        );
      }
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

export function ExportsRouteShell(): JSX.Element {
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const [searchParams, setSearchParams] = useSearchParams();
  const [mediaAssets, setMediaAssets] = useState<MediaAssetSummary[]>([]);
  const [jobs, setJobs] = useState<ExportJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [message, setMessage] = useMessage();
  const [pending, setPending] = useState(false);
  const [selectedMediaAssetId, setSelectedMediaAssetId] = useState(searchParams.get("media_asset_id") ?? "");
  const [youtubeOperation, setYoutubeOperation] = useState<"youtube_audio" | "youtube_video">("youtube_audio");
  const [audioBitrate, setAudioBitrate] = useState<(typeof AUDIO_BITRATES)[number]>(128);
  const [videoQuality, setVideoQuality] = useState<(typeof VIDEO_QUALITIES)[number]>("720p");

  const eligibleAssets = useMemo(
    () => mediaAssets.filter((asset) => isYouTubeAsset(asset) || isUploadedVideoAsset(asset)),
    [mediaAssets],
  );
  const selectedAsset = eligibleAssets.find((asset) => asset.media_asset_id === selectedMediaAssetId) ?? eligibleAssets[0];
  const activeExportJobKey = useMemo(
    () => jobs
      .filter((job) => ACTIVE_EXPORT_STATUSES.has(job.status))
      .map((job) => job.export_job_id)
      .sort()
      .join("|"),
    [jobs],
  );

  const reconcileJobs = useCallback((updates: ExportJob[]) => {
    setJobs((current) => reconcileExportJobs(current, updates));
  }, []);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [mediaResponse, jobsResponse] = await Promise.all([
        apiClient.listMediaAssets(channelAccountId, { pageSize: 50 }),
        apiClient.listExportJobs(channelAccountId, { pageSize: 50 }),
      ]);
      setMediaAssets(mediaResponse.items);
      setJobs(jobsResponse.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось загрузить экспорты.");
    } finally {
      setLoading(false);
    }
  }, [apiClient, channelAccountId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (!activeExportJobKey) {
      return undefined;
    }

    const exportJobIds = activeExportJobKey.split("|");
    let disposed = false;
    let timeoutId: ReturnType<typeof setTimeout> | undefined;

    const poll = async () => {
      const results = await Promise.allSettled(
        exportJobIds.map((exportJobId) => apiClient.getExportJob(channelAccountId, exportJobId)),
      );
      if (disposed) {
        return;
      }
      const updates = results.flatMap((result) => result.status === "fulfilled" ? [result.value] : []);
      if (updates.length > 0) {
        reconcileJobs(updates);
      }
      timeoutId = setTimeout(() => void poll(), EXPORT_JOB_POLL_INTERVAL_MS);
    };

    timeoutId = setTimeout(() => void poll(), EXPORT_JOB_POLL_INTERVAL_MS);
    return () => {
      disposed = true;
      if (timeoutId !== undefined) {
        clearTimeout(timeoutId);
      }
    };
  }, [activeExportJobKey, apiClient, channelAccountId, reconcileJobs]);

  const selectAsset = (mediaAssetId: string) => {
    setSelectedMediaAssetId(mediaAssetId);
    setSearchParams({ media_asset_id: mediaAssetId }, { replace: true });
  };

  const createExport = async () => {
    if (!selectedAsset) {
      setError("Выберите материал для скачивания.");
      return;
    }
    const operation: ExportOperation = isYouTubeAsset(selectedAsset) ? youtubeOperation : "video_to_audio";
    const variant: ExportVariant =
      operation === "youtube_video" ? { video_quality: videoQuality } : { audio_bitrate_kbps: audioBitrate };
    setPending(true);
    setError("");
    setMessage("");
    try {
      const job = await apiClient.createExportJob(channelAccountId, selectedAsset.media_asset_id, {
        operation,
        variant,
        deliveryChannel: "web",
        idempotencyKey: `web-export-${selectedAsset.media_asset_id}-${operation}-${Date.now()}`,
      });
      setMessage(`Задача добавлена: ${mediaAssetDisplayName(selectedAsset)}`);
      reconcileJobs([job]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось добавить задачу на экспорт.");
    } finally {
      setPending(false);
    }
  };

  const updateJob = async (job: ExportJob, action: "cancel" | "retry") => {
    setPending(true);
    setError("");
    setMessage("");
    try {
      const next =
        action === "cancel"
          ? await apiClient.cancelExportJob(channelAccountId, job.export_job_id)
          : await apiClient.retryExportJob(channelAccountId, job.export_job_id, `web-export-retry-${job.export_job_id}-${Date.now()}`);
      reconcileJobs([next]);
      setMessage(action === "cancel" ? "Остановка запрошена." : "Повтор добавлен в очередь.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось обновить задачу экспорта.");
    } finally {
      setPending(false);
    }
  };

  const download = async (job: ExportJob) => {
    setPending(true);
    setError("");
    try {
      const result = await apiClient.resolveExportDownload(channelAccountId, job.export_job_id);
      window.open(result.url, "_blank", "noopener,noreferrer");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось открыть файл.");
    } finally {
      setPending(false);
    }
  };

  return (
    <div className="page-grid page-grid--exports">
      <section className="surface surface--main">
        <SectionHeader
          action={<button className="secondary-button" onClick={() => void refresh()} type="button">Обновить</button>}
          eyebrow="Экспорт"
          title="Скачать или конвертировать"
        />
        <p className="helper-text">Экспорт не меняет текущие материалы и не запускает обработку.</p>
        {loading ? <p className="muted-text">Загружаем доступные материалы...</p> : null}
        {error ? <p className="error-text">{error}</p> : null}
        {message ? <p className="success-text">{message}</p> : null}
        {eligibleAssets.length === 0 && !loading ? <p className="muted-text">Нет материалов, доступных для экспорта.</p> : null}
        {selectedAsset ? (
          <div className="form-grid export-form">
            <label>
              Материал
              <select aria-label="Материал для экспорта" value={selectedAsset.media_asset_id} onChange={(event) => selectAsset(event.target.value)}>
                {eligibleAssets.map((asset) => (
                  <option key={asset.media_asset_id} value={asset.media_asset_id}>
                    {mediaAssetDisplayName(asset)}{isYouTubeAsset(asset) ? " · YouTube" : " · видео"}
                  </option>
                ))}
              </select>
            </label>
            {isYouTubeAsset(selectedAsset) ? (
              <div className="segmented" role="group" aria-label="Формат YouTube">
                <button aria-pressed={youtubeOperation === "youtube_audio"} className="segment-button" onClick={() => setYoutubeOperation("youtube_audio")} type="button">Аудио</button>
                <button aria-pressed={youtubeOperation === "youtube_video"} className="segment-button" onClick={() => setYoutubeOperation("youtube_video")} type="button">Видео</button>
              </div>
            ) : <p className="helper-text">Будет создан аудиофайл из загруженного видео.</p>}
            {isYouTubeAsset(selectedAsset) && youtubeOperation === "youtube_video" ? (
              <label>
                Качество видео
                <select aria-label="Качество видео" value={videoQuality} onChange={(event) => setVideoQuality(event.target.value as (typeof VIDEO_QUALITIES)[number])}>
                  {VIDEO_QUALITIES.map((quality) => <option key={quality} value={quality}>{quality}</option>)}
                </select>
              </label>
            ) : (
              <label>
                Качество аудио
                <select aria-label="Качество аудио" value={audioBitrate} onChange={(event) => setAudioBitrate(Number(event.target.value) as (typeof AUDIO_BITRATES)[number])}>
                  {AUDIO_BITRATES.map((bitrate) => <option key={bitrate} value={bitrate}>{bitrate} кбит/с</option>)}
                </select>
              </label>
            )}
            <button disabled={pending} onClick={() => void createExport()} type="button">
              {pending ? "Добавляем..." : isYouTubeAsset(selectedAsset) ? "Скачать" : "Конвертировать в аудио"}
            </button>
          </div>
        ) : null}
      </section>

      <aside className="side-stack">
        <section className="surface">
          <SectionHeader eyebrow="Задачи" title="Экспорт" />
          {jobs.length === 0 ? <p className="muted-text">Задач на экспорт пока нет.</p> : (
            <div className="data-list">
              {jobs.map((job) => {
                const source = mediaAssets.find((asset) => asset.media_asset_id === job.media_asset_id);
                return (
                  <article className="data-row export-job-row" key={job.export_job_id}>
                    <div className="row-main">
                      <strong>{source ? mediaAssetDisplayName(source) : "Материал"}</strong>
                      <p className="muted-text">{exportOperationLabel(job.operation)} · {exportVariantLabel(job.variant)}</p>
                      <p className="muted-text">{exportProgressLabel(job)}</p>
                    </div>
                    <dl className="row-meta">
                      <div><dt>Состояние</dt><dd><span className="status-pill" data-status={job.status}>{statusLabel(job.status)}</span></dd></div>
                      <div><dt>Попытка</dt><dd>{job.attempt_no} из {job.max_attempts}</dd></div>
                    </dl>
                    <div className="button-row export-job-actions">
                      {ACTIVE_EXPORT_STATUSES.has(job.status) ? <button className="secondary-button" disabled={pending || job.status === "cancel_requested"} onClick={() => void updateJob(job, "cancel")} type="button">Отменить</button> : null}
                      {RETRYABLE_EXPORT_STATUSES.has(job.status) ? <button className="secondary-button" disabled={pending} onClick={() => void updateJob(job, "retry")} type="button">Повторить</button> : null}
                      {job.status === "succeeded" ? <button disabled={pending} onClick={() => void download(job)} type="button">Скачать файл</button> : null}
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </section>
      </aside>
    </div>
  );
}

export function InboxRouteShell(): JSX.Element {
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const { collections, error, inbox, loading, mediaAssets, refresh, runs } = useInboxData();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [collectionName, setCollectionName] = useState("");
  const [targetCollectionId, setTargetCollectionId] = useState("");
  const [actionPending, setActionPending] = useState(false);
  const [actionMessage, setActionMessage] = useMessage();
  const [actionError, setActionError] = useState("");

  const activeRuns = runs.filter((run) => ACTIVE_RUN_STATUSES.has(run.status));

  const toggle = (mediaAssetId: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(mediaAssetId)) {
        next.delete(mediaAssetId);
      } else {
        next.add(mediaAssetId);
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelected(new Set(mediaAssets.map((item) => item.media_asset_id)));
  };

  const clearSelection = () => {
    setSelected(new Set());
  };

  const createCollection = async () => {
    setActionPending(true);
    setActionError("");
    setActionMessage("");
    try {
      const collection = await apiClient.createCollection(channelAccountId, {
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
      const existing = collection.items.map((item) => item.media_asset_id);
      const merged = Array.from(new Set([...existing, ...selected]));
      await apiClient.replaceCollectionItems(channelAccountId, collection.collection_id, {
        expectedVersion: collection.version,
        items: merged.map((media_asset_id, position) => ({ media_asset_id, position })),
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

  const removeMediaAsset = async (mediaAssetId: string) => {
    setActionPending(true);
    setActionError("");
    setActionMessage("");
    try {
      const removed = await apiClient.removeMediaAsset(channelAccountId, mediaAssetId);
      setSelected((current) => {
        const next = new Set(current);
        next.delete(mediaAssetId);
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
              <strong>{mediaAssets.length}</strong>
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
        <MediaAssetList
          items={mediaAssets}
          onClearSelection={clearSelection}
          onRemove={(mediaAssetId) => void removeMediaAsset(mediaAssetId)}
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
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const { collections, error, loading, mediaAssets, refresh } = useInboxData();
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
      const collection = await apiClient.createCollection(channelAccountId, {
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
      await apiClient.updateCollection(channelAccountId, collection.collection_id, {
        expectedVersion: collection.version,
        name: nextName,
      });
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Не удалось переименовать группу.");
    }
  };

  const removeItem = async (collection: Collection, mediaAssetId: string) => {
    setActionError("");
    try {
      await apiClient.removeCollectionItem(
        channelAccountId,
        collection.collection_id,
        mediaAssetId,
        collection.version,
      );
      await refresh();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Не удалось убрать материал.");
    }
  };

  const addItem = async (collection: Collection) => {
    const mediaAssetId = addTargets[collection.collection_id]!;
    setActionError("");
    try {
      const existing = collection.items.map((item) => item.media_asset_id);
      await apiClient.replaceCollectionItems(channelAccountId, collection.collection_id, {
        expectedVersion: collection.version,
        items: [...existing, mediaAssetId].map((item_id, position) => ({ media_asset_id: item_id, position })),
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
      await apiClient.updateCollection(channelAccountId, collection.collection_id, {
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
              {mediaAssets.map((item) => (
                <option key={item.media_asset_id} value={item.media_asset_id}>
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
                  <div className="mini-row" key={item.media_asset_id}>
                    <span>{item.media_asset?.display_name ?? "Материал"}</span>
                    <button
                      className="text-button danger"
                      onClick={() => void removeItem(collection, item.media_asset_id)}
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
                    {mediaAssets
                      .filter((item) => !collection.items.some((entry) => entry.media_asset_id === item.media_asset_id))
                      .map((item) => (
                        <option key={item.media_asset_id} value={item.media_asset_id}>
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
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const [searchParams] = useSearchParams();
  const { collections, error, loading, mediaAssets, refresh, runs } = useInboxData();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [sourceCollectionId, setSourceCollectionId] = useState("");
  const [runType, setRunType] = useState<RunType>("transcription");
  const [lastPlan, setLastPlan] = useState<{ selectionSnapshotId: string; runId: string } | null>(null);
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useMessage();
  const [runError, setRunError] = useState("");

  const collectionItems = useMemo(() => {
    const collection = collections.find((candidate) => candidate.collection_id === sourceCollectionId);
    return collection?.items.map((item) => item.media_asset_id) ?? [];
  }, [collections, sourceCollectionId]);

  const selectedItems = useMemo(
    () => mediaAssets.filter((item) => selected.has(item.media_asset_id)),
    [mediaAssets, selected],
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

  const toggle = (mediaAssetId: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(mediaAssetId)) {
        next.delete(mediaAssetId);
      } else {
        next.add(mediaAssetId);
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelected(new Set(mediaAssets.map((item) => item.media_asset_id)));
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
      const selection = await apiClient.createSelectionSnapshot(channelAccountId, {
        sourceCollectionId: sourceCollectionId || undefined,
        items: ids.map((media_asset_id, position) => ({ media_asset_id, position })),
        optionSnapshot: { basis: sourceCollectionId ? "collection" : "manual" },
      });
      const run = await apiClient.createAnalysisRun(channelAccountId, {
        selectionSnapshotId: selection.selection_snapshot_id,
        runType,
        params: undefined,
        delivery: { strategy: "polling" },
      });
      setLastPlan({ selectionSnapshotId: selection.selection_snapshot_id, runId: run.analysis_run_id });
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
        </div>
        <MediaAssetList
          items={mediaAssets}
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
              <div className="mini-row" key={item.media_asset_id}>
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
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
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
        apiClient.getAnalysisRun(channelAccountId, analysisRunId),
        apiClient.listAnalysisRunEvents(channelAccountId, analysisRunId, { pageSize: 50 }),
        apiClient.listArtifacts(channelAccountId, { analysisRunId, pageSize: 50 }),
      ]);
      const subjects: DiagnosticSubject[] = [
        { subject_type: "analysis_run", subject_id: analysisRunId },
        { subject_type: "selection_snapshot", subject_id: runResponse.selection_snapshot_id },
        ...runResponse.selection_snapshot.items.flatMap((item) => [
          { subject_type: "media_asset", subject_id: item.media_asset_id },
          { subject_type: "stored_object", subject_id: originObjectRef(item.origin_snapshot) },
        ]),
      ].filter((subject) => subject.subject_id);
      const diagnosticResponses = await Promise.all(
        subjects.map((subject) =>
          apiClient.listDiagnostics(channelAccountId, {
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
  }, [analysisRunId, apiClient, channelAccountId]);

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
      const next = await apiClient.cancelAnalysisRun(channelAccountId, analysisRunId);
      setRun(next);
      setMessage("Остановка запрошена");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось остановить запуск.");
    }
  };

  const retry = async () => {
    setMessage("");
    try {
      await apiClient.retryAnalysisRun(channelAccountId, analysisRunId);
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
                {run.selection_snapshot.items.map((item) => (
                  <div className="mini-row" key={`${item.media_asset_id}-${item.position}`}>
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
              <OriginDiagnosticsList run={run} diagnostics={diagnostics} />
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
              <article className="data-row outcome-row" key={`${item.media_asset_id}-${item.position}`}>
                <div className="row-main">
                  <strong>
                    #{item.position + 1} Материал
                  </strong>
                  <p className="muted-text">{item.selection_snapshot_item_id ? "Выбран в подборке" : "Элемент подборки"}</p>
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
                    <dd>{userVisibleArtifactLabels(item.artifact_kinds)}</dd>
                  </div>
                  <div>
                    <dt>Проверки</dt>
                    <dd>{item.diagnostic_ids?.length ?? diagnosticsForSubject(diagnostics, item.media_asset_id).length}</dd>
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
      {run.selection_snapshot.items.map((item) => {
        const itemDiagnostics = diagnosticsForSubject(diagnostics, item.media_asset_id);
        const originDiagnostics = diagnosticsForSubject(diagnostics, originObjectRef(item.origin_snapshot));
        return (
          <article className="data-row outcome-row" key={`${item.media_asset_id}-${item.position}`}>
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
                <dd>{originLabel(item.origin_snapshot)}</dd>
              </div>
              <div>
                <dt>Проверки</dt>
                <dd>{itemDiagnostics.length + originDiagnostics.length}</dd>
              </div>
            </dl>
          </article>
        );
      })}
    </div>
  );
}

function OriginDiagnosticsList({
  run,
  diagnostics,
}: {
  run: AnalysisRun;
  diagnostics: Diagnostic[];
}): JSX.Element {
  const entries = run.selection_snapshot.items.map((item) => {
    const itemDiagnostics = diagnosticsForSubject(diagnostics, item.media_asset_id);
    const originDiagnostics = diagnosticsForSubject(diagnostics, originObjectRef(item.origin_snapshot));
    return { item, diagnostics: [...itemDiagnostics, ...originDiagnostics] };
  });

  if (entries.every((entry) => entry.diagnostics.length === 0)) {
    return <p className="muted-text">Проверок по материалам нет.</p>;
  }

  return (
    <div className="data-list">
      {entries
        .filter((entry) => entry.diagnostics.length > 0)
        .map((entry) => (
          <article className="data-row origin-diagnostic-row" key={entry.item.media_asset_id}>
            <div className="row-main">
              <strong>{entry.item.display_name}</strong>
              <p className="muted-text">{originLabel(entry.item.origin_snapshot)}</p>
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
  const visibleArtifacts = userVisibleArtifacts(artifacts);

  if (visibleArtifacts.length === 0) {
    return <p className="muted-text">Результатов пока нет.</p>;
  }
  return (
    <div className="data-list">
      {visibleArtifacts.map((artifact) => (
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
  const visibleArtifacts = userVisibleArtifacts(artifacts);

  if (visibleArtifacts.length === 0) {
    return <p className="muted-text">Результатов пока нет.</p>;
  }

  const groups = visibleArtifacts.reduce<Record<string, ArtifactSummary[]>>((acc, artifact) => {
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
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const [artifacts, setArtifacts] = useState<ArtifactSummary[]>([]);
  const [artifact, setArtifact] = useState<Artifact | null>(null);
  const [diagnostics, setDiagnostics] = useState<Diagnostic[]>([]);
  const [error, setError] = useState("");
  const [message, setMessage] = useMessage();
  const [refreshingArtifact, setRefreshingArtifact] = useState(false);

  const refresh = useCallback(async () => {
    setError("");
    try {
      const response = await apiClient.listArtifacts(channelAccountId, { pageSize: 50 });
      setArtifacts(response.items);
      if (artifactId) {
        const [artifactResponse, diagnosticsResponse] = await Promise.all([
          apiClient.getArtifact(channelAccountId, artifactId),
          apiClient.listDiagnostics(channelAccountId, {
            subjectType: "artifact",
            subjectId: artifactId,
            pageSize: 50,
          }),
        ]);
        if (!isUserVisibleArtifactKind(artifactResponse.kind)) {
          setArtifact(null);
          setDiagnostics([]);
          setError("Этот служебный файл не показывается в обычных результатах.");
          return;
        }
        setArtifact(artifactResponse);
        setDiagnostics(diagnosticsResponse.items);
      } else {
        setArtifact(null);
        setDiagnostics([]);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось загрузить результаты.");
    }
  }, [apiClient, artifactId, channelAccountId]);

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
      const refreshed = await apiClient.refreshArtifact(channelAccountId, artifactId);
      setArtifact(refreshed);
      setMessage("Ссылка обновлена");
      const artifactsResponse = await apiClient.listArtifacts(channelAccountId, { pageSize: 50 });
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
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const [diagnostics, setDiagnostics] = useState<Diagnostic[]>([]);
  const [observability, setObservability] = useState<ObservabilitySnapshot | null>(null);
  const [subjectType, setSubjectType] = useState("");
  const [severity, setSeverity] = useState("");
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    setError("");
    try {
      const [response, snapshot] = await Promise.all([
        apiClient.listDiagnostics(channelAccountId, {
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
  }, [apiClient, channelAccountId, severity, subjectType]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

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
              <option value="channel_account">Канал</option>
              <option value="channel_surface">Поверхность</option>
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
      </aside>
    </div>
  );
}

export function MediaAssetDetailRouteShell(): JSX.Element {
  const { mediaAssetId = "" } = useParams();
  const { apiClient, env: { channelAccountId } } = useWebUiRuntime();
  const [item, setItem] = useState<MediaAsset | null>(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useMessage();
  const [removing, setRemoving] = useState(false);

  const refresh = useCallback(async () => {
    setError("");
    try {
      setItem(await apiClient.getMediaAsset(channelAccountId, mediaAssetId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось загрузить материал.");
    }
  }, [apiClient, channelAccountId, mediaAssetId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const removeMediaAsset = async () => {
    setError("");
    setMessage("");
    setRemoving(true);
    try {
      const removed = await apiClient.removeMediaAsset(channelAccountId, mediaAssetId);
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
              onClick={() => void removeMediaAsset()}
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
            <dt>Удален</dt>
            <dd>{formatDate(item.deleted_at)}</dd>
          </div>
          <div>
            <dt>Тип источника</dt>
            <dd>{kindLabel(item.origin.origin_type)}</dd>
          </div>
          <div>
            <dt>Создан</dt>
            <dd>{formatDate(item.created_at)}</dd>
          </div>
          <div>
            <dt>Размер</dt>
            <dd>{formatBytes(item.origin.size_bytes)}</dd>
          </div>
          <div>
            <dt>Откуда</dt>
            <dd>{assetOriginLabel(item)}</dd>
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
