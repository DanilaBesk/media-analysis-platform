#!/usr/bin/env python3
# FILE: infra/host-tools/deterministic-harness.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide a bounded deterministic host-harness command for live worker bridge smoke checks.
# SCOPE: Report markdown stdout, deep-research retained-artifact fixture writing, and claude-code style prompt parsing.
# DEPENDS: M-INFRA-COMPOSE, M-WORKER-REPORT, M-WORKER-DEEP-RESEARCH
# LINKS: V-M-WORKER-REPORT, V-M-WORKER-DEEP-RESEARCH, CUTOVER-WORKER-E2E
# ROLE: SCRIPT
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added deterministic host harness for API/MinIO/worker bridge smoke evidence.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   main - Emit deterministic report output or write deep-research retained artifacts for the requested phase.
# END_MODULE_MAP

from __future__ import annotations

import re
import sys
from pathlib import Path


_ARTIFACT_FIXTURES: dict[str, str] = {
    "evidence-research-state.md": "# Research State\n\n- gate: deterministic bridge smoke\n",
    "evidence-research-scope.md": "# Scope\n\nBounded worker bridge smoke.\n",
    "evidence-research-plan.md": "# Plan\n\n- Validate API claim/finalize and MinIO artifact flow.\n",
    "evidence-research-claim-registry.csv": "claim_id,status,summary\nclaim-1,verified,deterministic smoke\n",
    "evidence-research-graph-schema.md": "# Graph Schema\n\nNodes: claim, evidence, report.\n",
    "evidence-research-wave-log.md": "# Wave Log\n\n- Deterministic collection completed.\n",
    "evidence-research-source-registry.csv": "source_id,type,label\nsource-1,fixture,worker smoke\n",
    "evidence-research-evidence-registry.jsonl": '{"id":"evidence-1","status":"verified"}\n',
    "evidence-research-gap-log.md": "# Gap Log\n\nNo live bridge gaps found in deterministic smoke.\n",
    "evidence-research-claim-audit.csv": "claim_id,audit_status\nclaim-1,pass\n",
    "evidence-research-contradiction-register.md": "# Contradictions\n\nNone recorded for deterministic smoke.\n",
    "evidence-research-metrics.csv": "metric,value\nbridge_smoke,pass\n",
    "evidence-research-comparative-matrix.csv": "dimension,result\napi_minio_worker,pass\n",
    "evidence-research-risk-control-matrix.md": "# Risk Control Matrix\n\n- External harness quality not assessed.\n",
    "evidence-research-verification-checklist.md": "# Verification Checklist\n\n- [x] API child job succeeded.\n",
    "evidence-research-final-report.md": "# Deep Research Final Report\n\nDeterministic bridge smoke completed.\n",
}

__all__ = ["main"]


def main(argv: list[str]) -> int:
    prompt = _extract_prompt(argv)
    phase_id = _extract_phase_id(prompt)
    if phase_id:
        research_dir = _extract_research_dir(prompt)
        _write_deep_research_artifacts(research_dir)
        print(f"{phase_id}: continue: deterministic bridge smoke completed")
        return 0

    print(
        "# Deterministic Report\n\n"
        "## Summary\n\n"
        "This bounded host harness response proves the report worker bridge path, "
        "artifact rendering, MinIO persistence, and API finalization flow."
    )
    return 0


def _extract_prompt(argv: list[str]) -> str:
    if "--" in argv:
        separator = argv.index("--")
        if separator + 1 < len(argv):
            return argv[separator + 1]
    return argv[-1] if argv else ""


def _extract_phase_id(prompt: str) -> str | None:
    match = re.search(r"executing\s+(phase-\d{2}-[a-z-]+)\s+of", prompt)
    if match:
        return match.group(1)
    return None


def _extract_research_dir(prompt: str) -> Path:
    match = re.search(r"Research workspace:\s*(.+)", prompt)
    if match is None:
        raise RuntimeError("deep-research prompt does not include a research workspace")
    return Path(match.group(1).strip())


def _write_deep_research_artifacts(research_dir: Path) -> None:
    research_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in _ARTIFACT_FIXTURES.items():
        path = research_dir / filename
        path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
