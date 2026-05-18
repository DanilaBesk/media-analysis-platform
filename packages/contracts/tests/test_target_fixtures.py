from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "infra" / "fixtures" / "target" / "manifest.json"
UUID_RE = re.compile(r"^00000000-0000-4000-8000-[0-9]{12}$")


def test_target_fixture_manifest_has_stable_channel_accounts_and_media_bytes() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    fixtures = payload["fixtures"]

    channels = fixtures["channel_accounts"]
    assert [channel["channel"] for channel in channels] == ["local", "telegram", "web", "mcp"]
    for channel in channels:
        assert UUID_RE.match(channel["channel_account_id"])
        assert channel["external_account_ref"].strip()

    stored_objects = fixtures["stored_objects"]
    assert stored_objects
    for stored_object in stored_objects:
        path = ROOT / stored_object["fixture_path"]
        body = path.read_bytes()
        assert path.is_file()
        assert len(body) == stored_object["size_bytes"]
        assert hashlib.sha256(body).hexdigest() == stored_object["sha256"]
        assert not stored_object["object_key"].startswith("/")
        assert stored_object["bucket"] in {"media-inputs", "artifacts"}


def test_target_fixture_manifest_uses_target_vocabulary() -> None:
    text = MANIFEST.read_text(encoding="utf-8")
    forbidden_tokens = [
        '"owner"',
        "owner_type",
        "owner_id",
        "media_item",
        "selection_id",
        "analysis_run_task",
        "adapter_projection",
        "telegram_message_id",
    ]
    for token in forbidden_tokens:
        assert token not in text
