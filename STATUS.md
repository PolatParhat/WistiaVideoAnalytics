# Status: Wistia Video Analytics Pipeline
Updated: 2026-11-04 (end of Step 3 build)

## Where we are
Steps 0–3 done. The SME approved the design and its 4 points on 2026-11-04. Step 3 is built on branch
`worktree-step-3-build` in 11 stages (one commit each) and opened as a PR. Everything is verified locally,
**nothing is deployed to AWS yet**. Next: the user reviews and merges the PR, then runs Step 4 (deploy).

## Done this session
- SME approval recorded (a6520de).
- 3.1 single-file ingest, one API call (37b8a37) → 3.2 all endpoints, pagination, retries, JSON logs, raw files
  (4d4feaf) → 3.3 run_date, incremental window with 1-day lookback, local watermark (783362d).
- 3.4 refactor into library `src/wistia/` (6bfebbc) → 3.5 unit tests + ruff (d096193).
- 3.6 single PySpark transform raw → silver → gold, Iceberg MERGE, Glue 5.1 Docker runner (2f3cf59)
  → 3.7 pure transform functions + DQ checks in `src/wistia/transform/`, PySpark unit tests (f08d363).
- 3.8 AWS adapters (S3 write-once, DynamoDB forward-only watermark, Secrets Manager), Lambda handlers, Glue entry
  point (2ecc647; Glue entry tested in the Glue image).
- 3.9 CDK stack (78553d3) → 3.10 GitHub Actions CI + manual OIDC deploy (183f436) → 3.11 `simulate_week.py`,
  Athena evidence queries (8a31b2c).
- Verified: 50 unit tests + 6 CDK template tests pass; `cdk synth` OK; local simulated week = 39 events
  (new per day 9,5,6,5,3,3,8), rerun of 2026-03-20 adds 0, duplicate check = 0 in every table.

## Next actions
1. User reviews the Step 3 PR, waits for CI to go green, merges it, then in the main folder:
   `cd ~/Documents/DataEngineerBootcamp/Projects/WistiaVideoAnalytics && git pull`.
2. Step 4 deploy (user runs; README "Deploy to AWS"): `aws sts get-caller-identity` → `cdk bootstrap` →
   create secret `wistia/api-token` → `cd infra && cdk deploy WistiaPipeline -c alert_email=...` → confirm SNS email.
3. Step 5: `uv run python scripts/simulate_week.py --start 2026-03-17 --days 7 --rerun 2026-03-20`, then
   `uv run python scripts/athena_query.py sql/evidence.sql`; compare with the local numbers above.
4. Optional: GitHub deploy setup (`cdk deploy WistiaGithubOidc`, secret `AWS_DEPLOY_ROLE_ARN`, environment `production`).

## Open items
- Not yet verified in real AWS: Glue 5.1 + Iceberg Glue catalog, Athena reading the tables (column `date` in
  queries), the S3 `IfNoneMatch` write, the Step Functions flow. First deploy + first run will show it.
- A DQ failure in Glue is retried twice by Step Functions (same result); acceptable, costs ~$0.06.
- Merged remote branches that can be deleted (user only): `worktree-step-1-plan-api-exploration`,
  `worktree-step-2-drawio-diagram`, `worktree-step-2-sme-speaker-notes`, `worktree-step-2-video-link`.

## Environment gotchas
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
