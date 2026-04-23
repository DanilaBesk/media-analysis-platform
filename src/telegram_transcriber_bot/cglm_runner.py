# FILE: src/telegram_transcriber_bot/cglm_runner.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Preserve the legacy bot-facing report-generation import path while the canonical implementation lives in the dedicated report worker module.
# SCOPE: Compatibility bootstrap for `workers/report/src` and re-export of the extracted report worker helpers.
# DEPENDS: M-WORKER-REPORT
# LINKS: M-WORKER-REPORT, V-M-WORKER-REPORT
# ROLE: BARREL
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Converted the legacy cglm runner module into a compatibility re-export over the report worker package.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   report-worker-exports - Re-export the dedicated report worker helpers without changing the legacy import path.
# END_MODULE_MAP

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_report_worker_path() -> None:
    report_worker_src = Path(__file__).resolve().parents[2] / "workers" / "report" / "src"
    if report_worker_src.exists():
        report_worker_src_str = str(report_worker_src)
        if report_worker_src_str not in sys.path:
            sys.path.insert(0, report_worker_src_str)


# START_BLOCK_BLOCK_BOOTSTRAP_REPORT_WORKER_IMPORTS
try:
    import transcriber_worker_report as _report_worker
except ModuleNotFoundError:
    _ensure_report_worker_path()
    import transcriber_worker_report as _report_worker

sys.modules[__name__] = _report_worker
# END_BLOCK_BLOCK_BOOTSTRAP_REPORT_WORKER_IMPORTS
