# CopperASR Contracts, Security, Ops, And Submodule QA

Owner Bead: `media-b8s.3.2`

Status: passed after one non-ASR env-template local-path finding was fixed.

## Scope

This QA pass checks the maintainability and safety of the CopperASR integration:

- external submodule pin and ownership boundary;
- Docker/compose runtime topology;
- health, failure isolation, cancellation, and diagnostic contracts;
- model/provider provenance in artifacts;
- secrets and local-only path exposure in env examples;
- absence of first-party `apps/copper-asr` or editable `vendor/copper-asr`
  source ownership.

## Findings Fixed

`/.env.example` still used a machine-local value:

```dotenv
CGLM_BIN=/Users/danila/bin/cglm
```

That value is unrelated to ASR, but it is still a local-only path in a public env
template. It was replaced with an empty placeholder:

```dotenv
CGLM_BIN=
```

The previous no-legacy QA finding also removed root `WHISPER_*` entries from the
same file.

## Evidence

Submodule boundary:

```bash
git submodule status --recursive
git -C external/copper-asr rev-parse HEAD
git -C external/copper-asr status --short
git config --file .gitmodules --get-regexp '^submodule\..*\.(path|url)$'
```

Result:

- `external/copper-asr` is pinned at
  `f880151cfc57e082a94c028fb0d7483ccc1a921b`.
- `external/copper-asr` has no local source edits.
- `.gitmodules` path is `external/copper-asr` and URL is the upstream CopperASR
  GitLab repository.
- `vendor/copper-asr` and `apps/copper-asr` do not exist.

Compose/runtime security boundary:

```bash
docker compose -f infra/docker-compose.yml config
docker compose -f infra/docker-compose.yml ps copper-asr worker-transcription api
docker inspect --format '{{.Name}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}no-health{{end}}' infra-copper-asr-1 infra-worker-transcription-1 infra-api-1
```

Result:

- `copper-asr` builds from `infra/images/copper-asr/Dockerfile`, which copies
  `external/copper-asr`.
- `copper-asr` uses `expose: 8000`, not a host-published port.
- `worker-transcription` calls `http://copper-asr:8000` through
  `COPPER_ASR_BASE_URL`.
- `infra-copper-asr-1` is running and healthy.
- Local consumer resource controls are present:
  `COPPER_ASR_LOCAL_CPUS=4.0`, `COPPER_ASR_MAX_CONCURRENT_REQUESTS=1`,
  `COPPER_ASR_ONNX_NUM_THREADS=2`, `COPPER_ASR_TORCH_NUM_THREADS=2`,
  `COPPER_ASR_TORCH_INTEROP_THREADS=1`, and `COPPER_ASR_FFMPEG_THREADS=1`.
- The runtime image installs `external/copper-asr[server,cpu]`, keeping upstream
  ONNX VAD provider delegation on the CPU provider for this local compose
  profile.

Failure isolation:

```bash
python3 infra/scripts/copper-asr-failure-e2e.py --json --require-invalid-audio
```

Result:

- corrupt and retry runs fail with `diagnostic_codes=["asr_invalid_audio"]`;
- failed runs publish only `run_diagnostics` and `run_manifest`;
- cancellation reaches `status="canceled"` with `artifact_count=0`;
- runtime limits are exposed in proof output.

Model and artifact provenance:

```bash
python3 - <<'PY'
import json
from pathlib import Path
data = json.loads(Path("docs/benchmarks/copper-asr-long-voice-benchmark-latest.json").read_text())
print(data["backend"]["provider"], data["backend"]["model"], data["backend"]["revision"])
print(data["backend"]["metadata"]["processing"]["vad_segment_count"])
print(data["thresholds"]["passed"])
PY
```

Result:

- provider is `copperasr`;
- model is `Copperside/CoppersideASR`;
- revision is recorded as `null` when the provider does not supply one;
- VAD/ASR processing metadata is present;
- benchmark thresholds pass.

Secrets and local paths:

```bash
rg -n "/Users/danila|WHISPER_|faster-whisper|whisper-model-cache|PODLODKA|ctranslate" .env.example infra/env README.md pyproject.toml uv.lock workers apps infra/images infra/docker-compose.yml
```

Result: no active env, dependency, compose, image, worker, app, or README match
remains outside the no-legacy guard pattern itself.

Regression gates:

```bash
bash infra/scripts/no-legacy-asr-gate.sh
xmllint --noout docs/requirements.xml docs/technology.xml docs/development-plan.xml docs/verification-plan.xml docs/knowledge-graph.xml docs/operational-packets.xml
git diff --check
```

Result: all passed.

## Conclusion

The CopperASR integration is maintainable as an external pinned runtime source
under `external/copper-asr`, is not exposed as a public host service, has explicit
local resource controls, records provider/model provenance, isolates invalid
audio and cancellation correctly, and has no active ASR secret, local-path, or
legacy Whisper configuration leaks in the audited surfaces.
