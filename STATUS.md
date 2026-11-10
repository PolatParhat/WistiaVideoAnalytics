# Status: Wistia Video Analytics Pipeline
Updated: 2026-11-10 (Steps 4–5 done)

## Where we are
Steps 0–5 done. The pipeline is **deployed in AWS (us-east-1) and still running/deployed** (stack `WistiaPipeline`).
The simulated 7-day production run (FR8) passed: 8 Step Functions runs (2026-03-17 → 23 + rerun of 03-20) all
`SUCCEEDED`, 39 events, 0 duplicates. Evidence: `docs/05_production_run/README.md`. Steps 4.5 (GitHub deploy)
and 6 (dashboard) are deferred to the tutorial rebuild. Next: Step 7 (README done → video → teardown), then the book.

## Done this session (2026-11-10)
- PR #5 (Step 3 build) merged (fd8ef0f).
- Code walkthrough with the user in the VS Code debugger (ingestion, transform, AWS adapters). Debug configs live in
  `.claude/worktrees/step-3-build/.vscode/launch.json` (local only, not committed).
- Step 4 (user ran): `cdk bootstrap aws://<account-id>/us-east-1` → secret `wistia/api-token` → `cdk deploy
  WistiaPipeline` (CREATE_COMPLETE, ~3 min) → smoke test = day 1 (`simulate_week.py --start 2026-03-17 --days 1`).
- Step 5 (user ran): `simulate_week.py --start 2026-03-18 --days 6 --rerun 2026-03-20`; `athena_query.py
  sql/evidence.sql`. events_new per run 9,5,6,5,3,3,8 and 0 for the rerun; watermarks stay at 2026-03-23;
  only DQ failures are the known `reconcile:plays_vs_events` warnings. Matches the local run and Step 1 exactly.

## Next actions
1. User merges the Step 7 README PR, then `cd ~/Documents/DataEngineerBootcamp/Projects/WistiaVideoAnalytics && git pull`.
2. User confirms the SNS email subscription (still `PendingConfirmation` on 2026-11-10).
3. Step 7 video: user records the final walkthrough **while the stack is still deployed** (console: Step Functions
   history, S3 raw/, Athena evidence). Video stays local, never on GitHub.
4. Step 7 teardown (user, after the video): `cd infra && cdk destroy WistiaPipeline`, then delete the secret
   (README "Teardown"); optionally also the `CDKToolkit` bootstrap stack.
5. FINAL: the local-only tutorial book; its rebuild guide includes Step 4.5 (GitHub OIDC deploy) and Step 6
   (dashboard), both skipped in this project. Spec in Claude's project memory `final-step-tutorial-book`.

## Open items
- AWS stack is live: cost ~$0 idle + $0.40/month for the secret until teardown.
- SNS alert subscription not yet confirmed (see Next actions 2).
- A DQ failure in Glue is retried twice by Step Functions (same result); acceptable, costs ~$0.06.
- Merged remote branches that can be deleted (user only): `worktree-step-1-plan-api-exploration`,
  `worktree-step-2-drawio-diagram`, `worktree-step-2-sme-speaker-notes`, `worktree-step-2-video-link`,
  `worktree-step-3-build`.

## Environment gotchas
- AWS CLI default region on the Mac is **us-west-1**; the project is in **us-east-1**: always pass `--region us-east-1`
  and switch the console to N. Virginia.
- Deployed names: bucket `wistiapipeline-databuckete3889a50-dlqzxfwnylfh`, watermark table
  `WistiaPipeline-WatermarksC54B9051-O9YJKSI39TD7` (both change if the stack is recreated).
- Debug the PySpark transform in VS Code without Docker: venv Python + env `TZ=UTC` and
  `PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0 pyspark-shell"`.
- The SessionStart hook reads STATUS.md from the **main folder**: after merging a PR, run `git pull` there.
- `.env` (token) lives in the main project folder only; `jobs/ingest.py` finds it by walking up from the worktree.
- PySpark runs locally only through `scripts/spark_local.sh` (Glue 5.1 image, arm64). It keeps the image's
  PYTHONPATH (PyGlue.zip) and adds `/work/src`. The image has no PyYAML: the channel mapping is passed as JSON.
- Unit tests force UTC (`tests/conftest.py`), otherwise PySpark converts timestamps to the Mac's time zone.
- `cdk synth` bundles the Lambda with `uv pip install --python-platform aarch64-manylinux2014` (no Docker).
- Claude's worktree guard blocks complex shell commands (loops, `$(...)`, `source`): use plain commands.
- Export diagrams: `/Applications/draw.io.app/Contents/MacOS/draw.io -x -f png -s 2 -b 20 -o <out>.png <in>.drawio`
- Video (local, ignored): `docs/02_design/DesignExplinationVideo.mov` in the main folder. Never upload to GitHub.

## Key facts to keep straight
- Media IDs in scope: `8hunphufxp` (YouTube), `9k4tbcdfg0` (Facebook), from `config/media.yaml` (SME-confirmed).
- No dbt; AWS only; Python ingestion; PySpark transforms; GitHub Actions CI/CD.
- FR8 = simulated 7-day backfill on the real AWS pipeline, `run_date` 2026-03-17 → 23 (not 7 calendar days).
- Simulation week events per day: 8hunphufxp [0,1,1,3,1,3,2] = 11, 9k4tbcdfg0 [9,4,5,2,2,0,6] = 28 (total 39).
- Event natural key = `(event_key, visitor_key, received_at)`.
- Names: Spark catalog `lake`; Glue databases `wistia_silver|gold|ops`; secret `wistia/api-token`;
  stack `WistiaPipeline`; state machine `wistia-pipeline`; Glue job `wistia-transform`; Athena workgroup `wistia`.
- The GitHub repo is PUBLIC: the token goes only in `.env` / Secrets Manager / GitHub secrets, never in Git, chat or logs.
