from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_host_harness_server():
    module_path = Path(__file__).resolve().parents[1] / "infra" / "host-tools" / "host-harness-server.py"
    spec = importlib.util.spec_from_file_location("host_harness_server", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_codex_harness_type_translates_claude_style_print_args() -> None:
    server = _load_host_harness_server()

    command = server.build_harness_command(
        executable="/usr/local/bin/codex",
        harness_type="codex",
        args=[
            "-p",
            "--output-format",
            "text",
            "--permission-mode",
            "dontAsk",
            "--model",
            "gpt-5.4",
            "--add-dir",
            "/tmp/work",
            "/tmp/skill",
            "--",
            "Write a report.",
        ],
    )

    assert command == [
        "/usr/local/bin/codex",
        "exec",
        "--sandbox",
        "workspace-write",
        "--skip-git-repo-check",
        "--model",
        "gpt-5.4",
        "--add-dir",
        "/tmp/work",
        "--add-dir",
        "/tmp/skill",
        "Write a report.",
    ]


def test_claude_code_harness_type_preserves_args() -> None:
    server = _load_host_harness_server()

    command = server.build_harness_command(
        executable="/usr/local/bin/claude",
        harness_type="claude-code",
        args=["-p", "--", "Write a report."],
    )

    assert command == ["/usr/local/bin/claude", "-p", "--", "Write a report."]
