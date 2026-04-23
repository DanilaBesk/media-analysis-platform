// FILE: apps/web/tests/setup.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prepare the minimal browser-like assertions required for the app-local shell test harness.
// SCOPE: Install DOM matchers before route, env, and API-boundary tests run.
// DEPENDS: M-WEB-UI
// LINKS: V-M-WEB-UI
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added DOM matcher bootstrap for the Web UI shell test harness.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   install-dom-matchers - Extend Vitest assertions with DOM-specific expectations for the app shell tests.
// END_MODULE_MAP

import "@testing-library/jest-dom/vitest";
