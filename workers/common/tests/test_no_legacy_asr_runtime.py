from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

ACTIVE_PATHS = [
    "AGENTS.md",
    "CLAUDE.md",
    "README.md",
    "pyproject.toml",
    "uv.lock",
    "apps/api",
    "apps/mcp-server/src",
    "apps/telegram-bot/src",
    "apps/web/src",
    "infra/docker-compose.yml",
    "infra/env",
    "infra/images",
    "workers/agent-runner/src",
    "workers/common/src",
    "workers/common/tests",
    "workers/transcription/src",
    "workers/transcription/tests",
    "docs/technology.xml",
    "docs/knowledge-graph.xml",
    "docs/architecture/runtime-ops.md",
]

FORBIDDEN_TERMS = [
    "faster-" + "wh" + "isper",
    "faster_" + "wh" + "isper",
    "Whis" + "perTranscriber",
    "Default" + "Transcriber",
    "POD" + "LODKA_" + "WHIS" + "PER_MODEL",
    "WHIS" + "PER_MODEL",
    "WHIS" + "PER_DEVICE",
    "WHIS" + "PER_COMPUTE_TYPE",
    "WHIS" + "PER_MODEL_CACHE_DIR",
    "wh" + "isper-model-cache",
    "bond005/" + "wh" + "isper-pod" + "lodka-turbo",
    "ctranslate" + "2.converters.transformers",
]


def test_no_legacy_asr_runtime_terms_in_active_code_and_docs() -> None:
    violations: list[str] = []
    forbidden = [(term, term.casefold()) for term in FORBIDDEN_TERMS]
    for path in _iter_active_files():
        text = _read_text(path)
        if text is None:
            continue
        folded = text.casefold()
        for term, folded_term in forbidden:
            if folded_term in folded:
                violations.append(f"{path.relative_to(ROOT)} contains {term}")

    assert violations == []


def _iter_active_files() -> list[Path]:
    files: list[Path] = []
    for relative in ACTIVE_PATHS:
        path = ROOT / relative
        if path.is_file():
            files.append(path)
            continue
        if path.is_dir():
            files.extend(item for item in path.rglob("*") if item.is_file() and "__pycache__" not in item.parts)
    return sorted(files)


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None
