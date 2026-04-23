# FILE: src/telegram_transcriber_bot/transcribers.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Preserve the existing bot-facing transcription imports while worker-common owns the reusable implementation.
# SCOPE: Compatibility bootstrap for the worker-common path plus re-export of the current transcription helpers.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-TRANSCRIPTION
# ROLE: BARREL
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Converted the legacy transcription module into a compatibility re-export over worker-common.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   transcription-exports - Re-export the shared transcription classes and helper functions without changing the legacy import path.
# END_MODULE_MAP

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_worker_common_path() -> None:
    worker_common_src = Path(__file__).resolve().parents[2] / "workers" / "common" / "src"
    if worker_common_src.exists():
        worker_common_src_str = str(worker_common_src)
        if worker_common_src_str not in sys.path:
            sys.path.insert(0, worker_common_src_str)


# START_BLOCK_BLOCK_BOOTSTRAP_WORKER_COMMON_IMPORTS
try:
    from transcriber_workers_common import transcribers as _worker_common_transcribers
except ModuleNotFoundError:
    _ensure_worker_common_path()
    from transcriber_workers_common import transcribers as _worker_common_transcribers

sys.modules[__name__] = _worker_common_transcribers
# END_BLOCK_BLOCK_BOOTSTRAP_WORKER_COMMON_IMPORTS
