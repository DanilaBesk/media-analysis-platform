#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "${ROOT_DIR}"

MARKER='[NoLegacyASRGate]'

printf '%s validating active ASR runtime, dependency, env, compose, and docs surfaces\n' "${MARKER}"
uv run pytest workers/common/tests/test_no_legacy_asr_runtime.py -q
printf '%s active ASR surfaces exclude removed legacy runtime references\n' "${MARKER}"
