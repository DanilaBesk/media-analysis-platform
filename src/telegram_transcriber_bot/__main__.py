from __future__ import annotations

import sys


CUTOVER_MESSAGE = (
    "The legacy single-process runtime was replaced during cutover.\n"
    "Use the compose stack instead:\n"
    "  bash infra/scripts/compose-smoke.sh --live-smoke\n"
    "  docker compose -f infra/docker-compose.yml up -d\n"
    "See docs/architecture/cutover-checklist.md and docs/architecture/runtime-ops.md."
)


def main() -> int:
    print(CUTOVER_MESSAGE, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
