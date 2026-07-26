from __future__ import annotations

import json
import os
from pathlib import Path

from transcriber_workers_common.workspace import attempt_workspace, reap_abandoned_workspaces, remove_workspace


def test_attempt_workspace_is_unique_and_removed_after_success(tmp_path: Path) -> None:
    with attempt_workspace(tmp_path, "run/unsafe") as first:
        first_path = first
        assert first.is_relative_to(tmp_path.resolve())
        assert first.name.startswith("run-unsafe--")
        (first / "source.bin").write_bytes(b"media")
    assert not first_path.exists()

    with attempt_workspace(tmp_path, "run/unsafe") as second:
        assert second != first_path


def test_attempt_workspace_is_removed_after_failure(tmp_path: Path) -> None:
    try:
        with attempt_workspace(tmp_path, "run-1") as workspace:
            workspace_path = workspace
            raise RuntimeError("failed")
    except RuntimeError:
        pass
    assert not workspace_path.exists()


def test_attempt_workspace_uses_fenced_api_attempt_token(tmp_path: Path) -> None:
    with attempt_workspace(tmp_path, "job-1", attempt_token="api/token") as workspace:
        assert workspace.name == "job-1--api-token"
        lease = json.loads((workspace / ".attempt.json").read_text(encoding="utf-8"))
        assert lease["attempt_token"] == "api-token"
        assert lease["heartbeat_at"] >= lease["created_at"]


def test_reaper_removes_only_old_attempt_directories(tmp_path: Path) -> None:
    old = tmp_path / "old--attempt"
    recent = tmp_path / "recent--attempt"
    old.mkdir()
    recent.mkdir()
    os.utime(old, (100.0, 100.0))
    os.utime(recent, (950.0, 950.0))

    removed = reap_abandoned_workspaces(
        tmp_path,
        orphan_grace_seconds=100.0,
        absolute_ttl_seconds=1000.0,
        now=1000.0,
    )

    assert removed == (old,)
    assert not old.exists()
    assert recent.exists()


def test_reaper_preserves_recent_heartbeat_until_absolute_ttl(tmp_path: Path) -> None:
    active = tmp_path / "active--attempt"
    expired = tmp_path / "expired--attempt"
    active.mkdir()
    expired.mkdir()
    (active / ".attempt.json").write_text(
        json.dumps({"created_at": 200.0, "heartbeat_at": 990.0}), encoding="utf-8"
    )
    (expired / ".attempt.json").write_text(
        json.dumps({"created_at": 0.0, "heartbeat_at": 990.0}), encoding="utf-8"
    )

    removed = reap_abandoned_workspaces(
        tmp_path,
        orphan_grace_seconds=100.0,
        absolute_ttl_seconds=900.0,
        now=1000.0,
    )

    assert removed == (expired,)
    assert active.exists()


def test_cleanup_failure_is_observable_without_raising() -> None:
    def fail(_: Path) -> None:
        raise OSError("busy")

    assert remove_workspace(Path("/tmp/not-used"), remover=fail) is False
