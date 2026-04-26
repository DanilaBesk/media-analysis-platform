# FILE: workers/transcription/tests/test_transcription_worker_common_content_reuse.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Reuse the canonical worker-common transcription and document helper verification slices inside the transcription worker packet-local suite.
# SCOPE: Re-export shared transcript/docx/report-rendering and transcriber-runtime tests so the transcription packet gate covers the exact helper behavior it depends on.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON
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
#   reused-worker-common-document-tests - Re-export canonical worker-common document rendering tests for the transcription packet gate.
#   reused-worker-common-transcriber-tests - Re-export canonical worker-common transcriber tests for the transcription packet gate.
# END_MODULE_MAP

from workers.common.tests.test_worker_common_documents import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_documents_rendering import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_transcribers import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_transcribers_runtime import *  # noqa: F401,F403
