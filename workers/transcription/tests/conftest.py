# FILE: workers/transcription/tests/conftest.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Make the packet-local transcription worker tests import the new worker module together with shared worker-common and legacy source modules.
# SCOPE: Python path bootstrap for `workers/transcription/src`, `workers/common/src`, and `src`.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local pytest bootstrap for the transcription worker tests.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   _ensure_path - Insert a repository-local Python source root when it exists.
#   bootstrap-pythonpath - Add transcription worker, worker-common, and legacy src roots for packet-local tests.
# END_MODULE_MAP

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_path(path: Path) -> None:
    resolved = str(path)
    if path.exists() and resolved not in sys.path:
        sys.path.insert(0, resolved)


# START_BLOCK_BLOCK_BOOTSTRAP_PACKET_LOCAL_PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parents[3]
_ensure_path(REPO_ROOT / "workers" / "transcription" / "src")
_ensure_path(REPO_ROOT / "workers" / "common" / "src")
_ensure_path(REPO_ROOT / "src")
# END_BLOCK_BLOCK_BOOTSTRAP_PACKET_LOCAL_PYTHONPATH
