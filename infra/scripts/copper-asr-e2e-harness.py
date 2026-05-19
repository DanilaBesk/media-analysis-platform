#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = ROOT / "infra" / "fixtures" / "target" / "manifest.json"
REQUIRED_CASES = [
    "short_voice",
    "representative_long_voice",
    "corrupt_audio",
    "cancellation_voice",
    "artifact_download",
]


class HarnessError(RuntimeError):
    pass


def _load_manifest() -> dict[str, Any]:
    with MANIFEST_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stored_objects(fixtures: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item["stored_object_id"]): item for item in fixtures.get("stored_objects", [])}


def _validate_fixture_file(stored_object: dict[str, Any]) -> dict[str, Any]:
    fixture_path = ROOT / str(stored_object["fixture_path"])
    if not fixture_path.is_file():
        raise HarnessError(f"fixture file is missing: {fixture_path.relative_to(ROOT)}")

    body_size = fixture_path.stat().st_size
    expected_size = int(stored_object["size_bytes"])
    if body_size != expected_size:
        raise HarnessError(
            f"fixture size mismatch for {fixture_path.relative_to(ROOT)}: got {body_size}, expected {expected_size}"
        )

    digest = _sha256(fixture_path)
    expected_digest = str(stored_object["sha256"])
    if digest != expected_digest:
        raise HarnessError(
            f"fixture checksum mismatch for {fixture_path.relative_to(ROOT)}: got {digest}, expected {expected_digest}"
        )

    object_key = str(stored_object["object_key"])
    if object_key.startswith("/") or ".." in Path(object_key).parts:
        raise HarnessError(f"unsafe object key in manifest: {object_key}")

    return {
        "bucket": stored_object["bucket"],
        "object_key": object_key,
        "fixture_path": str(fixture_path.relative_to(ROOT)),
        "content_type": stored_object["content_type"],
        "size_bytes": body_size,
        "sha256": digest,
    }


def build_plan(*, copy_object_store_to: Path | None = None) -> dict[str, Any]:
    manifest = _load_manifest()
    fixtures = manifest["fixtures"]
    e2e = fixtures["copper_asr_e2e"]
    stored_by_id = _stored_objects(fixtures)

    if e2e["backend"] != "CopperASR":
        raise HarnessError("CopperASR fixture backend must be CopperASR")

    cases_by_id = {str(case["case_id"]): case for case in e2e.get("cases", [])}
    missing = [case_id for case_id in REQUIRED_CASES if case_id not in cases_by_id]
    if missing:
        raise HarnessError(f"CopperASR fixture manifest is missing required cases: {', '.join(missing)}")

    copied: list[dict[str, str]] = []
    planned_cases: list[dict[str, Any]] = []
    for case_id in REQUIRED_CASES:
        case = cases_by_id[case_id]
        stored_object_id = str(case["stored_object_id"])
        if stored_object_id not in stored_by_id:
            raise HarnessError(f"case {case_id} references unknown stored_object_id {stored_object_id}")
        stored_object = _validate_fixture_file(stored_by_id[stored_object_id])

        if copy_object_store_to is not None:
            target = copy_object_store_to / stored_object["bucket"] / stored_object["object_key"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / stored_object["fixture_path"], target)
            copied.append({"case_id": case_id, "target": str(target)})

        planned_cases.append(
            {
                "case_id": case_id,
                "owner_bead": case["owner_bead"],
                "media_kind": case["media_kind"],
                "stored_object_id": stored_object_id,
                "stored_object": stored_object,
                "expected_backend": case["expected_backend"],
                "assertions": case["assertions"],
            }
        )

    return {
        "backend": e2e["backend"],
        "fixture_manifest": str(MANIFEST_PATH.relative_to(ROOT)),
        "provenance_doc": e2e["provenance_doc"],
        "model_access_assumptions": e2e["model_access_assumptions"],
        "run_manifest_assertions": e2e["run_manifest_assertions"],
        "cases": planned_cases,
        "copied": copied,
        "commands": {
            "fixture_check": "python3 infra/scripts/copper-asr-e2e-harness.py --check-fixtures --json",
            "reset": "bash infra/scripts/target-reset-smoke.sh",
            "compose_config": "bash infra/scripts/compose-smoke.sh --check-config",
            "failure_e2e": "python3 infra/scripts/copper-asr-failure-e2e.py --json",
            "api_web_mcp_e2e": "python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json",
            "telegram_e2e": "python3 infra/scripts/copper-asr-telegram-e2e.py --json",
            "benchmark_e2e": "python3 infra/scripts/copper-asr-benchmark-e2e.py --json",
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate and prepare deterministic CopperASR E2E fixtures.")
    parser.add_argument("--check-fixtures", action="store_true", help="Validate manifest entries, files, sizes, and hashes.")
    parser.add_argument("--copy-object-store", type=Path, help="Copy fixture bytes into a bucket/object-key directory tree.")
    parser.add_argument("--json", action="store_true", help="Emit a machine-readable fixture plan.")
    args = parser.parse_args(argv)

    try:
        plan = build_plan(copy_object_store_to=args.copy_object_store)
    except HarnessError as exc:
        print(f"[CopperAsrE2EHarness] {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(plan, indent=2, sort_keys=True))
    elif args.check_fixtures or args.copy_object_store:
        print(f"[CopperAsrE2EHarness] validated {len(plan['cases'])} CopperASR E2E fixture cases")
    else:
        parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
