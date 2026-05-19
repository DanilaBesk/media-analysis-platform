from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "infra" / "fixtures" / "target" / "manifest.json"
COPPER_ASR_HARNESS = ROOT / "infra" / "scripts" / "copper-asr-e2e-harness.py"
COPPER_ASR_FAILURE_E2E = ROOT / "infra" / "scripts" / "copper-asr-failure-e2e.py"
COPPER_ASR_API_WEB_MCP_E2E = ROOT / "infra" / "scripts" / "copper-asr-api-web-mcp-e2e.py"
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


def test_copper_asr_e2e_fixture_manifest_covers_required_inputs_and_hashes() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    fixtures = payload["fixtures"]
    e2e = fixtures["copper_asr_e2e"]

    assert e2e["backend"] == "CopperASR"
    assert e2e["run_manifest_assertions"]["transcription_backend"] == "copperasr"
    assert e2e["run_manifest_assertions"]["legacy_asr_allowed"] is False

    stored_by_id = {item["stored_object_id"]: item for item in fixtures["stored_objects"]}
    cases = {case["case_id"]: case for case in e2e["cases"]}
    assert set(cases) == {
        "short_voice",
        "representative_long_voice",
        "corrupt_audio",
        "cancellation_voice",
        "artifact_download",
    }

    for case in cases.values():
        assert case["expected_backend"] == "CopperASR"
        assert case["owner_bead"].startswith("media-b8s.2.")
        assert "backend_is_copperasr" in case["assertions"]
        stored_object = stored_by_id[case["stored_object_id"]]
        path = ROOT / stored_object["fixture_path"]
        assert path.is_file()
        assert len(path.read_bytes()) == stored_object["size_bytes"]
        assert stored_object["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()

    assert cases["short_voice"]["media_kind"] == "voice"
    assert cases["representative_long_voice"]["benchmark_role"] == "representative_long_voice"
    assert cases["corrupt_audio"]["expected_diagnostic_code"] == "asr_invalid_audio"
    assert cases["corrupt_audio"]["accepted_live_diagnostic_codes"] == [
        "asr_invalid_audio",
        "asr_unexpected_runtime_error",
    ]
    assert cases["cancellation_voice"]["cancellation_checkpoint"] == "before_or_during_asr"
    assert cases["artifact_download"]["assertions"] == ["artifact_download_non_empty", "backend_is_copperasr"]


def test_copper_asr_e2e_harness_reports_deterministic_fixture_plan() -> None:
    result = subprocess.run(
        [sys.executable, str(COPPER_ASR_HARNESS), "--check-fixtures", "--json"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["backend"] == "CopperASR"
    assert payload["fixture_manifest"] == "infra/fixtures/target/manifest.json"
    assert [case["case_id"] for case in payload["cases"]] == [
        "short_voice",
        "representative_long_voice",
        "corrupt_audio",
        "cancellation_voice",
        "artifact_download",
    ]
    assert payload["commands"]["reset"] == "bash infra/scripts/target-reset-smoke.sh"
    assert payload["commands"]["compose_config"] == "bash infra/scripts/compose-smoke.sh --check-config"
    assert payload["commands"]["failure_e2e"] == "python3 infra/scripts/copper-asr-failure-e2e.py --json"
    assert payload["commands"]["api_web_mcp_e2e"] == "python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json"
    assert COPPER_ASR_FAILURE_E2E.is_file()
    assert COPPER_ASR_FAILURE_E2E.stat().st_mode & 0o111
    assert COPPER_ASR_API_WEB_MCP_E2E.is_file()
    assert COPPER_ASR_API_WEB_MCP_E2E.stat().st_mode & 0o111
