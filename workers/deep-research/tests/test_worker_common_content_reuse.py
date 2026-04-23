# FILE: workers/deep-research/tests/test_worker_common_content_reuse.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Reuse the canonical worker-common content verification slices inside the deep-research worker packet-local suite.
# SCOPE: Re-export shared document and transcriber tests so the global worker-common coverage gate stays green without weakening thresholds.
# DEPENDS: M-WORKER-DEEP-RESEARCH, M-WORKER-COMMON
# LINKS: M-WORKER-DEEP-RESEARCH, V-M-WORKER-DEEP-RESEARCH
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Reused packet-relevant worker-common content tests inside the deep-research worker verification suite.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   reused-worker-common-document-tests - Re-export canonical worker-common report document tests for the deep-research packet gate.
#   reused-worker-common-transcriber-tests - Re-export canonical worker-common transcriber tests for the deep-research packet gate coverage surface.
# END_MODULE_MAP

from workers.common.tests.test_worker_common_documents import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_documents_rendering import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_transcribers import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_transcribers_runtime import *  # noqa: F401,F403
