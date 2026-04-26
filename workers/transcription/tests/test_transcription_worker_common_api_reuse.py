# FILE: workers/transcription/tests/test_transcription_worker_common_api_reuse.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Reuse the canonical worker-common API and artifact verification slices inside the transcription worker packet-local suite so the packet gate exercises the shared control contract it depends on.
# SCOPE: Re-export the shared API transport, DTO validation, and artifact helper tests without reopening worker-common implementation scope.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.1 - Gave the reuse shim a worker-specific basename for all-worker pytest collection.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   reused-worker-common-api-tests - Re-export canonical worker-common API contract tests for the transcription packet gate.
#   reused-worker-common-artifact-tests - Re-export canonical worker-common artifact helper tests for the transcription packet gate.
# END_MODULE_MAP

from workers.common.tests.test_api import *  # noqa: F401,F403
from workers.common.tests.test_api_transport import *  # noqa: F401,F403
from workers.common.tests.test_artifacts import *  # noqa: F401,F403
from workers.common.tests.test_object_store import *  # noqa: F401,F403
from workers.common.tests.test_runtime import *  # noqa: F401,F403
