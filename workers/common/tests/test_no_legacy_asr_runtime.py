from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

ACTIVE_PATHS = [
    ".env.example",
    "AGENTS.md",
    "CLAUDE.md",
    "README.md",
    "pyproject.toml",
    "uv.lock",
    "apps/api",
    "apps/api/go.mod",
    "apps/api/go.sum",
    "apps/mcp-server/package.json",
    "apps/mcp-server/pnpm-lock.yaml",
    "apps/mcp-server/src",
    "apps/telegram-bot/pyproject.toml",
    "apps/telegram-bot/uv.lock",
    "apps/telegram-bot/src",
    "apps/web/package.json",
    "apps/web/pnpm-lock.yaml",
    "apps/web/src",
    "infra/docker-compose.yml",
    "infra/env",
    "infra/images",
    "workers/agent-runner/src",
    "workers/common/src",
    "workers/common/tests",
    "workers/transcription/src",
    "workers/transcription/tests",
    ".grace/context/technology.xml",
    ".grace/context/deployment.xml",
    ".grace/graph/main.xml",
    ".grace/verification/main.xml",
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


def test_no_legacy_asr_gate_is_single_ci_command_wired_into_inventory() -> None:
    gate = ROOT / "infra/scripts/no-legacy-asr-gate.sh"
    coverage_inventory = ROOT / "infra/scripts/coverage-inventory.sh"
    target_gate = ROOT / "infra/scripts/no-legacy-target-gate.sh"

    assert gate.is_file()
    assert gate.stat().st_mode & 0o111

    gate_text = gate.read_text(encoding="utf-8")
    assert "workers/common/tests/test_no_legacy_asr_runtime.py" in gate_text

    inventory_text = coverage_inventory.read_text(encoding="utf-8")
    assert "bash infra/scripts/no-legacy-asr-gate.sh" in inventory_text

    target_gate_text = target_gate.read_text(encoding="utf-8")
    assert "bash infra/scripts/no-legacy-asr-gate.sh" in target_gate_text


def test_copper_asr_runtime_image_keeps_cpu_onnx_runtime_extra() -> None:
    dockerfile = ROOT / "infra/images/copper-asr/Dockerfile"
    env_example = ROOT / "infra/env/copper-asr.env.example"

    dockerfile_text = dockerfile.read_text(encoding="utf-8")
    env_text = env_example.read_text(encoding="utf-8")

    assert '".[server,cpu]"' in dockerfile_text
    assert "onnxruntime-gpu" not in dockerfile_text
    assert "COPPER_ASR_ONNX_NUM_THREADS=4" in env_text
    assert "COPPER_ASR_TORCH_NUM_THREADS=4" in env_text
    assert "COPPER_ASR_FFMPEG_THREADS=2" in env_text
    assert "COPPER_ASR_CACHE_DIR=/var/cache/copper-asr" in dockerfile_text
    assert "COPPER_ASR_CACHE_DIR=/var/cache/copper-asr" in env_text
    assert "TORCH_HOME" not in dockerfile_text
    assert "TORCH_HOME" not in env_text


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
