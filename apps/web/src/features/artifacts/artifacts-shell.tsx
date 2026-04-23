// FILE: apps/web/src/features/artifacts/artifacts-shell.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Implement the artifact resolution surface for the Web UI job-details route while keeping API truth authoritative.
// SCOPE: Render artifact summaries, resolve presigned downloads through the API, and surface artifact refresh errors without client-side guessing.
// DEPENDS: M-WEB-UI, M-API-HTTP, M-CONTRACTS
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Replaced the artifact placeholder with API-backed resolution and download rendering.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   render-artifact-list - Present artifact summaries from the authoritative job snapshot.
//   resolve-download-handle - Re-read artifact download handles through the API instead of guessing stale client state.
// END_MODULE_MAP

import { useJobDetailsRouteModel } from "../jobs/jobs-shell";

function formatDate(value: string): string {
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

// START_BLOCK_BLOCK_RENDER_ARTIFACTS_ROUTE_SHELL
export function ArtifactsRouteShell(): JSX.Element {
  const model = useJobDetailsRouteModel();
  const artifacts = model.snapshot?.artifacts ?? [];

  return (
    <section className="shell-card" aria-label="Artifacts shell">
      <h3>Artifacts</h3>
      {artifacts.length === 0 ? <p className="muted-text">No artifacts are available yet.</p> : null}
      <div className="artifact-list">
        {artifacts.map((artifact) => {
          const resolution = model.artifactResolutions[artifact.artifact_id];
          return (
            <article className="artifact-row" key={artifact.artifact_id}>
              <div>
                <strong>{artifact.artifact_kind}</strong>
                <p className="muted-text">{artifact.filename}</p>
                <p className="muted-text">
                  {artifact.mime_type} - {formatBytes(artifact.size_bytes)} - {formatDate(artifact.created_at)}
                </p>
              </div>
              <div className="shell-stack">
                <button
                  className="action-button"
                  disabled={model.resolvingArtifactId === artifact.artifact_id}
                  onClick={() => void model.resolveArtifact(artifact.artifact_id)}
                >
                  {model.resolvingArtifactId === artifact.artifact_id
                    ? "Resolving..."
                    : "Resolve download"}
                </button>
                {resolution ? (
                  <a className="job-link" href={resolution.download.url} rel="noreferrer" target="_blank">
                    Download via presigned URL
                  </a>
                ) : null}
                {resolution ? (
                  <p className="helper-text">Expires {formatDate(resolution.download.expires_at)}</p>
                ) : null}
              </div>
            </article>
          );
        })}
      </div>
      {model.artifactError ? <p className="error-text">{model.artifactError}</p> : null}
      <p className="helper-text">
        Artifact links are always re-resolved through the API, so expired URLs are refreshed from server
        truth instead of being inferred locally.
      </p>
    </section>
  );
}
// END_BLOCK_BLOCK_RENDER_ARTIFACTS_ROUTE_SHELL
