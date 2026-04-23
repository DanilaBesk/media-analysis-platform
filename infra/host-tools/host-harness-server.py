#!/usr/bin/env python3
# FILE: infra/host-tools/host-harness-server.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Run a configurable developer-local AI CLI harness on behalf of Docker workers that cannot execute host binaries inside Linux containers.
# SCOPE: Localhost HTTP bridge, bearer-token guard, harness-type dispatch, subprocess execution, timeout handling, and JSON result envelopes.
# DEPENDS: M-INFRA-COMPOSE, M-WORKER-REPORT, M-WORKER-DEEP-RESEARCH
# LINKS: V-M-INFRA-COMPOSE, V-M-WORKER-REPORT, V-M-WORKER-DEEP-RESEARCH
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added a host-only bridge for invoking configurable local AI CLI harnesses from compose workers without downloading or packaging SDKs.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   HostHarnessBridgeHandler - Accept authenticated local bridge requests and return subprocess result envelopes.
#   build_harness_command - Convert worker CLI requests into the selected host harness CLI contract.
#   main - Start the localhost bridge for the configured HOST_HARNESS_COMMAND and HOST_HARNESS_TYPE.
# END_MODULE_MAP

from __future__ import annotations

import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


_LOG_MARKER = "[InfraCompose][hostHarnessBridge][BLOCK_RUN_HOST_HARNESS_BRIDGE]"
_SUPPORTED_HARNESS_TYPES = {"claude-code", "codex", "passthrough"}

__all__ = ["HostHarnessBridgeHandler", "build_harness_command", "main"]


class HostHarnessBridgeHandler(BaseHTTPRequestHandler):
    server_version = "HostHarnessBridge/1.0"

    def do_POST(self) -> None:
        if self.path != "/run":
            self._write_json(404, {"error": "not_found"})
            return
        if not self._is_authorized():
            self._write_json(401, {"error": "unauthorized"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            args = _validate_args(payload.get("args"))
            request_type = _validate_harness_type(payload.get("harness_type"))
            bridge_type = _resolve_harness_type()
            if request_type != bridge_type:
                raise ValueError(f"requested harness type {request_type!r} does not match bridge type {bridge_type!r}")
            cwd = _validate_cwd(payload.get("cwd"))
            completed = _run_harness(args=args, cwd=cwd, harness_type=bridge_type)
        except Exception as exc:
            self._write_json(400, {"error": str(exc)})
            return

        self._write_json(
            200,
            {
                "exit_code": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            },
        )

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"{_LOG_MARKER} {self.address_string()} {fmt % args}")

    def _is_authorized(self) -> bool:
        expected = os.getenv("HOST_HARNESS_TOKEN", "").strip()
        if not expected:
            return False
        return self.headers.get("Authorization", "") == f"Bearer {expected}"

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _validate_args(value: object) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("args must be a list of strings")
    return list(value)


def _validate_harness_type(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("harness_type must be a non-empty string")
    harness_type = value.strip()
    if harness_type not in _SUPPORTED_HARNESS_TYPES:
        raise ValueError(f"unsupported harness type {harness_type!r}")
    return harness_type


def _resolve_harness_type() -> str:
    return _validate_harness_type(os.getenv("HOST_HARNESS_TYPE", "claude-code"))


def _validate_cwd(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("cwd must be a string when provided")
    path = Path(value)
    return str(path) if path.exists() else None


def _run_harness(*, args: list[str], cwd: str | None, harness_type: str) -> subprocess.CompletedProcess[str]:
    executable = os.getenv("HOST_HARNESS_COMMAND", "").strip()
    timeout = float(os.getenv("HOST_HARNESS_TIMEOUT_SECONDS", "3660"))
    if not executable:
        raise ValueError("HOST_HARNESS_COMMAND is empty")
    if not Path(executable).exists():
        raise FileNotFoundError(f"host harness command not found: {executable}")

    command = build_harness_command(executable=executable, args=args, harness_type=harness_type)
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
        env=dict(os.environ),
    )


def build_harness_command(*, executable: str, args: list[str], harness_type: str) -> list[str]:
    if harness_type in {"claude-code", "passthrough"}:
        return [executable, *args]
    if harness_type == "codex":
        prompt, add_dirs, model = _parse_claude_style_print_args(args)
        command = [executable, "exec", "--sandbox", "workspace-write", "--skip-git-repo-check"]
        if model:
            command.extend(["--model", model])
        for add_dir in add_dirs:
            command.extend(["--add-dir", add_dir])
        command.append(prompt)
        return command
    raise ValueError(f"unsupported harness type {harness_type!r}")


def _parse_claude_style_print_args(args: list[str]) -> tuple[str, list[str], str | None]:
    prompt_separator_index = args.index("--") if "--" in args else -1
    prompt = args[prompt_separator_index + 1] if prompt_separator_index >= 0 and len(args) > prompt_separator_index + 1 else ""
    if not prompt:
        raise ValueError("codex host harness requires a prompt argument")

    option_args = args[:prompt_separator_index] if prompt_separator_index >= 0 else args[:-1]
    add_dirs: list[str] = []
    model: str | None = None
    index = 0
    while index < len(option_args):
        item = option_args[index]
        if item == "--add-dir":
            index += 1
            while index < len(option_args) and not option_args[index].startswith("-"):
                add_dirs.append(option_args[index])
                index += 1
            continue
        if item == "--model" and index + 1 < len(option_args):
            model = option_args[index + 1]
            index += 2
            continue
        index += 1
    return prompt, add_dirs, model


def main() -> int:
    host = os.getenv("HOST_HARNESS_HOST", "127.0.0.1")
    port = int(os.getenv("HOST_HARNESS_PORT", "8765"))
    if not os.getenv("HOST_HARNESS_TOKEN", "").strip():
        raise SystemExit("HOST_HARNESS_TOKEN must be set")
    harness_type = _resolve_harness_type()
    if not os.getenv("HOST_HARNESS_COMMAND", "").strip():
        raise SystemExit("HOST_HARNESS_COMMAND must be set")
    print(f"{_LOG_MARKER} listening host={host} port={port} harness_type={harness_type}")
    ThreadingHTTPServer((host, port), HostHarnessBridgeHandler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
