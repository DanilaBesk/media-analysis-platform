from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


CommandRunner = Callable[[list[str]], str]


@dataclass(frozen=True, slots=True)
class DeepResearchPhase:
    phase_id: str
    reference_path: str
    required_artifacts: tuple[tuple[str, ...], ...]


PHASES: tuple[DeepResearchPhase, ...] = (
    DeepResearchPhase("phase-00-activation", "references/phase-00-activation.md", (("evidence-research-state.md",),)),
    DeepResearchPhase(
        "phase-01-scope-freeze",
        "references/phase-01-scope-freeze.md",
        (("evidence-research-state.md",), ("evidence-research-scope.md",)),
    ),
    DeepResearchPhase(
        "phase-02-decomposition",
        "references/phase-02-decomposition.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-plan.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-graph-schema.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-03-wave-collection",
        "references/phase-03-wave-collection.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-wave-log.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-source-registry.csv",),
            ("evidence-research-evidence-registry.jsonl",),
        ),
    ),
    DeepResearchPhase(
        "phase-04-gap-analysis",
        "references/phase-04-gap-analysis.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-gap-log.md",),
            ("evidence-research-plan.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-05-follow-up-collection",
        "references/phase-05-follow-up-collection.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-wave-log.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-source-registry.csv",),
            ("evidence-research-evidence-registry.jsonl",),
            ("evidence-research-gap-log.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-06-claim-audit",
        "references/phase-06-claim-audit.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-claim-audit.csv",),
        ),
    ),
    DeepResearchPhase(
        "phase-07-contradiction-pass",
        "references/phase-07-contradiction-pass.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-contradiction-register.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-claim-audit.csv",),
            ("evidence-research-gap-log.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-08-normalization",
        "references/phase-08-normalization.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-metrics.csv",),
            ("evidence-research-comparative-matrix.csv",),
            ("evidence-research-graph-schema.md", "evidence-research-graph.json"),
            ("evidence-research-evidence-registry.jsonl",),
        ),
    ),
    DeepResearchPhase(
        "phase-09-synthesis",
        "references/phase-09-synthesis.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-risk-control-matrix.md",),
            ("evidence-research-final-report.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-10-verification",
        "references/phase-10-verification.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-verification-checklist.md",),
            ("evidence-research-final-report.md",),
            ("evidence-research-risk-control-matrix.md",),
        ),
    ),
)


PROMPT_TEMPLATE = """You are executing {phase_id} of a deterministic deep research pipeline for a Telegram bot research job.

Primary source artifacts:
- Transcript markdown: {transcript_path}
- Initial analytical report markdown: {report_path}
- Original source label: {source_label}

Working directory for this run:
- Research workspace: {research_dir}

Normative method references that you must read before acting:
- Skill entrypoint: {skill_root}/SKILL.md
- Control plane: {skill_root}/references/control-plane.md
- Artifact taxonomy: {skill_root}/references/artifact-taxonomy.md
- Final output contract: {skill_root}/references/final-output-contract.md
- Artifact templates: {skill_root}/templates/artifact-templates.md
- Current phase instructions: {skill_root}/{phase_reference}
- Research design basis: {design_doc}

Execution contract:
- This run is `evidence-retained`, not `report-only`.
- Work only through deterministic retained artifacts inside {research_dir}.
- Use the transcript and initial report as seed evidence, but broaden the study with external evidence when the phase permits collection.
- Follow the phase model and gate discipline from the evidence-first orchestration method.
- Use bounded subagents only when the current phase explicitly allows delegation.
- Do not collapse the run into one summary. Preserve the full research memory pack.
- Every material finding must trace to claim IDs, evidence packets, contradiction records, or explicit unresolved status.
- Keep the tone formal, analytical, and machine-usable.

Required artifacts for this phase:
{required_artifacts}

Report emphasis for this project:
- Reconstruct the vector of thought, not only conclusions.
- Preserve question -> answer -> discussion progression.
- Capture every distinct theme, idea, objection, comparison, and unresolved issue present in the transcript and follow-up evidence.
- Make agreements, disagreements, and boundary notes explicit.

Return contract:
- Update the phase artifacts on disk.
- Record the gate result in `evidence-research-state.md`.
- Return only one line in the form: `{phase_id}: <continue|repeat|stop>: <short summary>`.
"""


def build_deep_research_command(
    transcript_path: Path,
    report_path: Path,
    research_dir: Path,
    phase: DeepResearchPhase,
    source_label: str,
) -> list[str]:
    skill_root = _skill_root()
    design_doc = _design_doc()
    required_artifacts = "\n".join(f"- {' or '.join(option)}" for option in phase.required_artifacts)
    prompt = PROMPT_TEMPLATE.format(
        phase_id=phase.phase_id,
        transcript_path=transcript_path.resolve(),
        report_path=report_path.resolve(),
        source_label=source_label or "Unknown source",
        research_dir=research_dir.resolve(),
        skill_root=skill_root.resolve(),
        phase_reference=phase.reference_path,
        design_doc=design_doc.resolve(),
        required_artifacts=required_artifacts,
    ).strip()

    return [
        _resolve_claude_executable(),
        "-p",
        "--output-format",
        "text",
        "--permission-mode",
        "acceptEdits",
        "--model",
        _deep_research_model(),
        "--add-dir",
        str(research_dir.resolve()),
        str(transcript_path.parent.resolve()),
        str(skill_root.resolve()),
        str(design_doc.parent.resolve()),
        "--",
        prompt,
    ]


def run_deep_research(
    transcript_path: Path,
    report_path: Path,
    research_dir: Path,
    source_label: str,
    command_runner: CommandRunner | None = None,
) -> Path:
    if not transcript_path.exists():
        raise FileNotFoundError(f"Transcript file does not exist: {transcript_path}")
    if not report_path.exists():
        raise FileNotFoundError(f"Research report seed does not exist: {report_path}")

    research_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = research_dir / "phase-logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    runner = command_runner or _run_command
    for phase in PHASES:
        output = runner(
            build_deep_research_command(
                transcript_path=transcript_path,
                report_path=report_path,
                research_dir=research_dir,
                phase=phase,
                source_label=source_label,
            )
        ).strip()
        (logs_dir / f"{phase.phase_id}.log").write_text((output + "\n") if output else "", encoding="utf-8")
        gate_status, gate_summary = _parse_gate_result(output, phase.phase_id)
        _validate_phase_artifacts(research_dir, phase)
        if gate_status == "repeat":
            raise RuntimeError(f"{phase.phase_id} requested repeat: {gate_summary}")
        if gate_status == "stop" and phase != PHASES[-1]:
            raise RuntimeError(f"{phase.phase_id} stopped the pipeline early: {gate_summary}")

    final_report = research_dir / "evidence-research-final-report.md"
    if not final_report.exists():
        raise RuntimeError("Deep research pipeline completed without a final report artifact.")
    return final_report


def _validate_phase_artifacts(research_dir: Path, phase: DeepResearchPhase) -> None:
    for options in phase.required_artifacts:
        candidates = [research_dir / name for name in options]
        if any(candidate.exists() and candidate.stat().st_size > 0 for candidate in candidates):
            continue
        label = " or ".join(options)
        raise RuntimeError(f"{phase.phase_id} did not produce required artifact: {label}")


def _parse_gate_result(output: str, phase_id: str) -> tuple[str, str]:
    match = re.search(rf"{re.escape(phase_id)}:\s*(continue|repeat|stop)\s*:\s*(.+)", output, re.IGNORECASE)
    if match is None:
        raise RuntimeError(f"{phase_id} did not return a valid gate result line.")
    return match.group(1).lower(), match.group(2).strip()


def _skill_root() -> Path:
    return Path(
        os.getenv(
            "DEEP_RESEARCH_SKILL_ROOT",
            "/Users/danila/.agents/skills/evidence-first-research-orchestrator",
        )
    )


def _design_doc() -> Path:
    return Path(
        os.getenv(
            "DEEP_RESEARCH_DESIGN_DOC",
            "/Users/danila/work/my/ai-workflow/docs/research/2026-04-04-evidence-first-deep-research-system-design.md",
        )
    )


def _deep_research_model() -> str:
    return os.getenv("DEEP_RESEARCH_MODEL", "sonnet").strip() or "sonnet"


def _resolve_claude_executable() -> str:
    configured = os.getenv("DEEP_RESEARCH_BIN", "").strip()
    if configured:
        return configured

    resolved = shutil.which("claude")
    if resolved:
        return resolved

    fallback = Path("/opt/homebrew/bin/claude")
    if fallback.exists():
        return str(fallback)

    raise RuntimeError("claude executable not found. Set DEEP_RESEARCH_BIN or add claude to PATH.")


def _build_command_env() -> dict[str, str]:
    env = dict(os.environ)
    path_entries = [
        str(Path.home() / "bin"),
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ]
    existing = env.get("PATH", "")
    if existing:
        path_entries.extend(part for part in existing.split(":") if part)

    normalized: list[str] = []
    seen: set[str] = set()
    for part in path_entries:
        if not part or part in seen:
            continue
        seen.add(part)
        normalized.append(part)
    env["PATH"] = ":".join(normalized)
    return env


def _run_command(command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=int(os.getenv("DEEP_RESEARCH_TIMEOUT_SECONDS", "5400")),
            env=_build_command_env(),
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("deep research pipeline timed out") from exc
    if completed.returncode != 0:
        raise RuntimeError(f"deep research command failed with exit code {completed.returncode}: {completed.stderr.strip()}")
    return completed.stdout
