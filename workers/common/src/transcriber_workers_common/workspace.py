from __future__ import annotations

import json
import logging
import shutil
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER = "[WorkerCommon][workspace][workspace_cleanup_failed]"
_LEASE_FILE = ".attempt.json"


def safe_workspace_token(value: str) -> str:
    stripped = value.strip()
    cleaned = "".join(character if character.isalnum() or character in {"-", "_", "."} else "-" for character in stripped)
    cleaned = cleaned.strip("-_.")
    return (cleaned or "attempt")[:96]


@contextmanager
def attempt_workspace(
    workspace_root: Path,
    subject_id: str,
    *,
    attempt_token: str | None = None,
    heartbeat_interval_seconds: float = 10.0,
) -> Iterator[Path]:
    root = Path(workspace_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    workspace_token = safe_workspace_token(attempt_token or uuid.uuid4().hex)
    workspace = (root / f"{safe_workspace_token(subject_id)}--{workspace_token}").resolve()
    if not workspace.is_relative_to(root):
        raise ValueError("attempt workspace resolved outside workspace_root")
    workspace.mkdir(mode=0o700)
    lease_file = workspace / _LEASE_FILE
    created_at = time.time()
    _write_lease(lease_file, subject_id=subject_id, attempt_token=workspace_token, created_at=created_at, heartbeat_at=created_at)
    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=_heartbeat_lease,
        args=(lease_file, subject_id, workspace_token, created_at, stop_heartbeat, heartbeat_interval_seconds),
        daemon=True,
    )
    heartbeat.start()
    try:
        yield workspace
    finally:
        stop_heartbeat.set()
        heartbeat.join(timeout=max(heartbeat_interval_seconds, 0.1) + 1.0)
        remove_workspace(workspace)


def remove_workspace(workspace: Path, *, remover: Callable[[Path], None] | None = None) -> bool:
    delete = remover or _remove_tree
    try:
        delete(Path(workspace))
    except FileNotFoundError:
        return True
    except Exception:
        _LOGGER.exception("%s workspace=%s", _LOG_MARKER, workspace)
        return False
    return True


def reap_abandoned_workspaces(
    workspace_root: Path,
    *,
    orphan_grace_seconds: float,
    absolute_ttl_seconds: float,
    now: float | None = None,
) -> tuple[Path, ...]:
    root = Path(workspace_root).resolve()
    if not root.exists():
        return ()
    current_time = time.time() if now is None else now
    removed: list[Path] = []
    for candidate in root.iterdir():
        if not candidate.is_dir():
            continue
        try:
            created_at, heartbeat_at = _workspace_times(candidate)
        except FileNotFoundError:
            continue
        if current_time - created_at < absolute_ttl_seconds and current_time - heartbeat_at < orphan_grace_seconds:
            continue
        if remove_workspace(candidate):
            removed.append(candidate)
    return tuple(removed)


def _remove_tree(path: Path) -> None:
    shutil.rmtree(path)


def _write_lease(lease_file: Path, *, subject_id: str, attempt_token: str, created_at: float, heartbeat_at: float) -> None:
    temporary = lease_file.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "subject_id": subject_id,
                "attempt_token": attempt_token,
                "created_at": created_at,
                "heartbeat_at": heartbeat_at,
            }
        ),
        encoding="utf-8",
    )
    temporary.replace(lease_file)


def _heartbeat_lease(
    lease_file: Path,
    subject_id: str,
    attempt_token: str,
    created_at: float,
    stop: threading.Event,
    interval_seconds: float,
) -> None:
    interval = max(interval_seconds, 0.1)
    while not stop.wait(interval):
        try:
            _write_lease(
                lease_file,
                subject_id=subject_id,
                attempt_token=attempt_token,
                created_at=created_at,
                heartbeat_at=time.time(),
            )
        except FileNotFoundError:
            return
        except Exception:
            _LOGGER.exception("%s lease_file=%s", _LOG_MARKER, lease_file)


def _workspace_times(workspace: Path) -> tuple[float, float]:
    lease_file = workspace / _LEASE_FILE
    try:
        data = json.loads(lease_file.read_text(encoding="utf-8"))
        created_at = float(data["created_at"])
        heartbeat_at = float(data.get("heartbeat_at", created_at))
        return created_at, heartbeat_at
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        modified_at = workspace.stat().st_mtime
        return modified_at, modified_at
