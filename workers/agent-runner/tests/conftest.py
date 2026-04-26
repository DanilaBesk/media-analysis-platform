# FILE: workers/agent-runner/tests/conftest.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Make packet-local agent-runner tests import the new worker module and shared worker-common package.
# SCOPE: Python path bootstrap for `workers/agent-runner/src`, `workers/common/src`, and `src`.
# DEPENDS: M-WORKER-AGENT-RUNNER, M-WORKER-COMMON
# LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local pytest bootstrap for the agent-runner worker tests.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   _ensure_path - Insert a repository-local Python source root when it exists.
#   bootstrap-pythonpath - Add agent-runner worker, worker-common, and legacy src roots for tests.
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
_ensure_path(REPO_ROOT / "workers" / "agent-runner" / "src")
_ensure_path(REPO_ROOT / "workers" / "common" / "src")
_ensure_path(REPO_ROOT / "src")
# END_BLOCK_BLOCK_BOOTSTRAP_PACKET_LOCAL_PYTHONPATH
