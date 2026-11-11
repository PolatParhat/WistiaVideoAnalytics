# Status: Wistia Video Analytics Pipeline
Updated: 2026-11-10 (Step 7 done, AWS torn down)

## Where we are
Steps 0–7 done. The pipeline was deployed, ran the simulated 7-day production week (8 runs SUCCEEDED, 39 events,
0 duplicates; evidence in `docs/05_production_run/README.md`), the final walkthrough video was recorded (local only),
and **everything was deleted from AWS** (account checked clean on 2026-11-10). Next: the FINAL step, the local-only
tutorial book.

## Done this session (2026-11-10)
- PR #5 (Step 3 build) merged. Code walkthrough in the VS Code debugger.
- Step 4: `cdk bootstrap` → secret `wistia/api-token` → `cdk deploy WistiaPipeline` → smoke test (day 1).
- Step 5: simulated week + rerun of 2026-03-20; evidence and report results (PR #6).
- Step 7: README results + FR1–FR12 coverage (PR #7); walkthrough speaker notes (PRs #8–#10), then removed from Git
  and git-ignored (PR #11; local copies in `docs/02_design/` and `docs/07_walkthrough/` of the main folder and in
  `~/Documents/DataEngineerBootcamp/Books/WistiaVideoAnalytics/speaker_notes/`); video recorded (local only).
- Teardown (user ran): `cdk destroy WistiaPipeline`; secret deleted; 4 leftover log groups deleted; `CDKToolkit`
  stack deleted; its retained, versioned assets bucket emptied (all versions + delete markers) and deleted.

## Next actions
1. FINAL: the local-only tutorial book in `~/Documents/DataEngineerBootcamp/Books/WistiaVideoAnalytics/` (every tool,
   data-engineering perspective, examples + practice, project walkthrough, rebuild guide 3.1 → 3.11 including
   Step 4.5 GitHub OIDC deploy and Step 6 dashboard). Spec in Claude's project memory `final-step-tutorial-book`.

## Open items
- AWS: nothing left from this project. One unconfirmed SNS email subscription to the deleted alert topic still shows
  (`PendingConfirmation`); AWS removes it by itself within 3 days, no cost.
- Not ours, left alone: Athena default workgroup `primary`; SNS topic `s3-s3-cross-region-migration`.
- Merged remote branches that can be deleted (user only): `worktree-step-1-plan-api-exploration`,
  `worktree-step-2-drawio-diagram`, `worktree-step-2-sme-speaker-notes`, `worktree-step-2-video-link`,
  `worktree-step-3-build`, and the Step 4–7 `worktree-*` branches.
- Speaker notes are still readable in old commits of the public history (no secrets in them); rewriting history was
  not done.

## Environment gotchas
- AWS CLI default region on the Mac is **us-west-1**; the project is in **us-east-1**: always pass `--region us-east-1`.
- Rebuilding on AWS starts from zero: `cdk bootstrap aws://<account-id>/us-east-1`, then `create-secret` (token in `.env`).
- The CDK assets bucket is retained and versioned: after deleting `CDKToolkit`, delete all object versions, then the bucket.
- In zsh, `$VAR` holding several words is not split: write options like `--region us-east-1` out in full.
- Debug the PySpark transform in VS Code without Docker: venv Python + env `TZ=UTC` and
  `PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0 pyspark-shell"`.
- The SessionStart hook reads STATUS.md from the **main folder**: after merging a PR, run `git pull` there.
- `.env` (token) lives in the main project folder only; `jobs/ingest.py` finds it by walking up from the worktree.
- PySpark runs locally through `scripts/spark_local.sh` (Glue 5.1 image, arm64). The image has no PyYAML: the channel
  mapping is passed as JSON.
- Unit tests force UTC (`tests/conftest.py`), otherwise PySpark converts timestamps to the Mac's time zone.
- `cdk synth` bundles the Lambda with `uv pip install --python-platform aarch64-manylinux2014` (no Docker).
- Claude's worktree guard blocks complex shell commands (loops, `$(...)`, `source`): use plain commands.
- Videos (local, ignored): `docs/02_design/DesignExplinationVideo.mov` and the final walkthrough. Never upload to GitHub.

## Key facts to keep straight
- Media IDs in scope: `8hunphufxp` (YouTube), `9k4tbcdfg0` (Facebook), from `config/media.yaml` (SME-confirmed).
- No dbt; AWS only; Python ingestion; PySpark transforms; GitHub Actions CI/CD.
- FR8 = simulated 7-day backfill on the real AWS pipeline, `run_date` 2026-03-17 → 23 (not 7 calendar days).
- Simulation week events per day: 8hunphufxp [0,1,1,3,1,3,2] = 11, 9k4tbcdfg0 [9,4,5,2,2,0,6] = 28 (total 39).
- Event natural key = `(event_key, visitor_key, received_at)`.
- Names: Spark catalog `lake`; Glue databases `wistia_silver|gold|ops`; secret `wistia/api-token`;
  stack `WistiaPipeline`; state machine `wistia-pipeline`; Glue job `wistia-transform`; Athena workgroup `wistia`.
- The GitHub repo is PUBLIC: the token goes only in `.env` / Secrets Manager / GitHub secrets, never in Git, chat or logs.
