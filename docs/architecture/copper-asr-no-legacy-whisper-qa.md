# CopperASR No-Legacy Whisper QA

Owner Bead: `media-b8s.3.1`

Status: passed after one active env-template finding was fixed.

## Scope

This QA pass independently checks that the CopperASR migration no longer leaves
active Whisper, faster-whisper, Podlodka, CTranslate2, or `DefaultTranscriber`
runtime paths in code, dependency manifests, env examples, compose, runtime
wrappers, and recent local logs.

Allowed remaining references are limited to:

- no-legacy guard patterns and negative assertions;
- historical migration, research, benchmark, or readiness wording that describes
  the removed legacy runtime instead of configuring or calling it.

## Finding Fixed

`/.env.example` still declared:

- `WHISPER_MODEL`
- `WHISPER_DEVICE`
- `WHISPER_COMPUTE_TYPE`

Those entries were active env-template configuration and were removed. The
no-legacy regression test now includes `/.env.example` in its active scan set so
the same leak fails `bash infra/scripts/no-legacy-asr-gate.sh`.

## Evidence

Passed:

```bash
bash infra/scripts/no-legacy-asr-gate.sh
```

Result: `2 passed`; active ASR surfaces exclude removed legacy runtime
references.

Passed:

```bash
rg -n -i "whisper|faster[-_]?whisper|ctranslate|podlodka|DefaultTranscriber|WhisperTranscriber|WHISPER" apps workers packages infra pyproject.toml uv.lock README.md .env.example --glob '!infra/fixtures/**' --glob '!**/__pycache__/**'
```

Remaining matches are guard/negative checks or historical benchmark comparison:

- `infra/scripts/compose-smoke.sh` reject rules for legacy snippets.
- CopperASR E2E assertions that run manifests must not leak legacy ASR wording.
- `workers/common/tests/test_no_legacy_asr_runtime.py` split forbidden-token
  guard patterns.
- `infra/scripts/copper-asr-benchmark-e2e.py` `previous_backend` historical
  comparison metadata.

Passed:

```bash
docker compose -f infra/docker-compose.yml logs --tail=250 copper-asr worker-transcription api | rg -n -i "whisper|faster[-_]?whisper|ctranslate|podlodka|DefaultTranscriber|WhisperTranscriber|backend_unavailable"
```

Result: no recent `copper-asr`, `worker-transcription`, or `api` log matches for
legacy ASR or `backend_unavailable`.

Passed:

```bash
git diff --check
```

## Conclusion

No active legacy Whisper runtime path remains in the audited surfaces. The only
active issue found during QA was fixed and covered by the no-legacy gate.
