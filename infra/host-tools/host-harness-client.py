#!/usr/bin/env python3
# FILE: infra/host-tools/host-harness-client.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide a container-local CLI shim that forwards worker subprocess calls to a configurable host AI harness.
# SCOPE: CLI argument forwarding, harness type metadata, bridge authentication header, stdout/stderr replay, and exit-code preservation.
# DEPENDS: M-INFRA-COMPOSE, M-WORKER-REPORT, M-WORKER-DEEP-RESEARCH
# LINKS: V-M-INFRA-COMPOSE, V-M-WORKER-REPORT, V-M-WORKER-DEEP-RESEARCH
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added a stdlib-only host harness client so Docker workers can invoke developer-local AI CLI harnesses without packaging host binaries into Linux containers.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   main - Forward current CLI argv to the configured host harness bridge and preserve stdout, stderr, and exit code.
# END_MODULE_MAP

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


__all__ = ["main"]


def main(argv: list[str]) -> int:
    bridge_url = os.getenv("HOST_HARNESS_BRIDGE_URL", "http://host.docker.internal:8765/run").strip()
    token = os.getenv("HOST_HARNESS_TOKEN", "").strip()
    harness_type = os.getenv("HOST_HARNESS_TYPE", "claude-code").strip()
    timeout = float(os.getenv("HOST_HARNESS_CLIENT_TIMEOUT_SECONDS", "3660"))
    if not bridge_url:
        print("host harness bridge URL is empty; set HOST_HARNESS_BRIDGE_URL", file=sys.stderr)
        return 127
    if not token:
        print("host harness token is empty; set HOST_HARNESS_TOKEN", file=sys.stderr)
        return 127
    if not harness_type:
        print("host harness type is empty; set HOST_HARNESS_TYPE", file=sys.stderr)
        return 127

    payload = json.dumps({"args": argv, "cwd": os.getcwd(), "harness_type": harness_type}).encode("utf-8")
    request = urllib.request.Request(
        bridge_url,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"host harness bridge HTTP {exc.code}: {detail}", file=sys.stderr)
        return 127
    except Exception as exc:
        print(f"host harness bridge unavailable: {exc}", file=sys.stderr)
        return 127

    stdout = body.get("stdout", "")
    stderr = body.get("stderr", "")
    if stdout:
        print(stdout, end="")
    if stderr:
        print(stderr, end="", file=sys.stderr)
    return int(body.get("exit_code", 1))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
