# Decisions and change log

History of the Wistia Video Analytics project. Current state: `STATUS.md`. Rules: `CLAUDE.md`.
Newest entries are at the end of each table/log.

## Decisions

| Date | Decision | Reason |
|---|---|---|
| 2026-11-02 | Original requirements .docx kept out of Git; redacted `requirements.md` committed instead | The .docx contains the Wistia API token marked "DO NOT SHARE" |
| 2026-11-02 | Wistia token stored in `.env` (git-ignored) | Secrets never go into the repo |
| 2026-11-03 | Compressed timeline: a couple of days instead of the doc's 6 weekly milestones | User needs it done fast; production quality is still the bar |
| 2026-11-03 | FR8 (7 consecutive production days) met by a simulated 7-day backfill: the deployed AWS pipeline runs 7 times, each with a `run_date`, incremental between runs | User decision (acts as SME). Same code and infra as a real daily schedule; also proves incremental + idempotent reruns. The daily schedule still exists in IaC. Deviation from the written requirement, stated openly in the README and video |
| 2026-11-03 | User acts as SME for the architecture approval | User decision, to keep the timeline |
| 2026-11-03 | Build-first, then teach: Claude builds after design approval; the user retypes everything in a separate practice folder | Production repo stays stable; learning copy can break safely |
| 2026-11-03 | GitHub repo https://github.com/PolatParhat/WistiaVideoAnalytics (public) | Provided by the user. Public, so secrets must never be committed; history checked before the first push |
| 2026-11-03 | Build in real-engineer evolution order (single script → common library → tests → CI → infra), one commit per stage | User request: learn how code grows in real life |
| 2026-11-03 | Natural key for events = `(event_key, visitor_key, received_at)` | Step 1: `event_key` repeats for different events (1 case per media) |
| 2026-11-03 | `dim_visitor` built from events, not `/stats/visitors` | Visitors endpoint is account-wide, has no media/date filter and no IP/country |
| 2026-11-03 | Masked samples only in the repo (IP cut to 2 octets, tokens removed) | Public repo; events contain IPs and locations |
| 2026-11-03 | Bearer auth (both Bearer and Basic work) | Wistia's documented method; one place in the client to switch |
| 2026-11-03 | `channel` per media from `config/media.yaml`: `9k4tbcdfg0` = Facebook, `8hunphufxp` = YouTube (assumption) | Wistia has no channel field; requirement asks for one per media |
| 2026-11-03 | `traffic_source` per event from `embed_url`: facebook_ad (fbclid / fbc_id / utm_source=meta), website, direct | Real signal in the data, unlike the assumed channel |
| 2026-11-03 | `fact_media_engagement` = media × visitor × UTC date from events; `play_rate` moved to new `fact_media_daily` (from `by_date`) | Loads exist only per media per day, so a per-visitor play rate cannot be computed |
| 2026-11-03 | Simulation window 2026-03-17 → 2026-03-23 | 39 events, ≥3 per day, both media active (Step 1 finding 6) |
| 2026-11-03 | Architecture proposed: EventBridge Scheduler (disabled) → Step Functions → Lambda ingest → S3 raw → Glue 5 PySpark → Iceberg (Glue Catalog) → Athena; DynamoDB watermark; Secrets Manager; CloudWatch + SNS; CDK; GitHub Actions with OIDC | Serverless, < $1 for the 7-day simulation, idempotent reruns via Iceberg MERGE. MWAA rejected on cost (~$0.49/h). Details: docs/02_design/README.md |
| 2026-11-03 | Incremental window = watermark + 1 day − 1-day lookback → run_date; watermark advanced only after transform + DQ succeed | Late events are re-read; upsert makes it safe; a failed run is caught up automatically |
| 2026-11-03 | Step 2 design approved | User (acting as SME) |
| 2026-11-03 | Diagrams drawn in draw.io (`.drawio` source + 2× PNG export in `docs/02_design/`), replacing Mermaid | User: Mermaid output quality too low for the deliverable; draw.io gives AWS icons and exact layout |
| 2026-11-03 | Walkthrough videos are not put on GitHub (not committed, not as release assets); shared with the SME directly. `*.mov`/`*.mp4` stay ignored | User decision: recordings are review material, not part of the code deliverable; keeps the public repo free of large binaries and of anything accidentally shown on screen |
| 2026-11-03 | Design submitted to the external SME; Step 3 waits for their approval | User decision: get real sign-off before building (the earlier 'user acts as SME' approval stands as the internal approval) |
| 2026-11-04 | SME approved the design and the 4 points: play_rate in `fact_media_daily`; channel `9k4tbcdfg0` = Facebook, `8hunphufxp` = YouTube; Bearer auth; simulated 7-day run (2026-03-17 → 23) | Reported by the user. Step 3 build starts |
| 2026-11-04 | Glue **5.1** instead of 5.0 (Spark 3.5.6, Python 3.11, Iceberg 1.10) | The local test image is `aws-glue-libs:5.1.0` (native arm64); deploying the same version means what is tested locally is what runs in AWS. Same Spark major version, no design change |
| 2026-11-04 | Channel mapping passed to Glue as a JSON job argument (`--media_config`), built from `config/media.yaml` at `cdk synth`; Lambda gets it as env var `MEDIA_CONFIG` | The Glue image has no PyYAML; one source of truth (the YAML) without extra Glue modules |
| 2026-11-04 | Iceberg catalog named `lake` everywhere (Hadoop catalog in `lake/warehouse` locally, Glue Data Catalog on S3 in AWS); Glue databases `wistia_silver`, `wistia_gold`, `wistia_ops` created by CDK | Identical SQL locally and in AWS; `cdk destroy` removes the databases with their tables |
| 2026-11-04 | Gold is rebuilt only for keys the run touched (media × days in the window, visitors seen), from all of silver, then `MERGE`d | Late events re-read by the lookback update existing gold rows; reruns are idempotent |
| 2026-11-04 | DQ checks run after the `MERGE`; a failed critical check marks the run failed in `ops.pipeline_runs`, fails the Glue job, and the watermark does not move | Simple and visible; the next run re-processes the same window. Plays-vs-events reconciliation is a warning: Wistia's daily `play_count` and the event count differ slightly (e.g. 8 vs 9 on 2026-03-17) |
| 2026-11-04 | Raw files are write-once: `IfNoneMatch="*"` on S3 put, refuse-overwrite locally; every ingestion run has its own `run_id` folder | Raw stays immutable even when a step is retried |
| 2026-11-04 | Watermark only moves forward (DynamoDB conditional write) | Rerunning an old day (e.g. 2026-03-20 after 2026-03-23) must not undo later loads |
| 2026-11-04 | Lambda code bundled locally with `uv pip install --python-platform aarch64-manylinux2014` (no Docker at synth) | Works the same on the Mac and in GitHub Actions |
| 2026-11-04 | Bucket, table and log groups use `RemovalPolicy.DESTROY` (+ auto-delete objects) | Learning project: `cdk destroy` in Step 7 must leave nothing that costs money |
| 2026-11-04 | GitHub deploy via OIDC role (`WistiaGithubOidc` stack) limited to this repo's `production` environment and to assuming the CDK bootstrap roles | No long-lived AWS keys in GitHub; least privilege |
| 2026-11-04 | Daily schedule loads **yesterday** (`scheduled_time` − 1 day); manual/simulated runs pass `run_date` | Today's data is incomplete at 02:00 UTC |
| 2026-11-10 | Smoke test = day 1 of the simulated week (2026-03-17); days 2–7 + rerun in a second `simulate_week.py` call | The first real run is not thrown away; `ops.pipeline_runs` holds exactly the 7 days + the rerun |
| 2026-11-10 | Tutorial "book" (every tool, data-engineering perspective, examples + practice, project walkthrough, rebuild guide) is the **final** step, kept local only (not in Git) | User decision: finish the project first, learn from the book afterwards |
| 2026-11-10 | GitHub Actions deploy (OIDC role, `AWS_DEPLOY_ROLE_ARN`, `production` environment) not set up in this project; done during the rebuild from the tutorial | User decision, to finish faster. The Deploy workflow and OIDC stack are built and synthesized; the real deployment ran from the CLI |
| 2026-11-10 | No dashboard (optional FR11); report queries + results in docs/05_production_run instead; dashboard built during the rebuild | User decision, to finish faster |
| 2026-11-10 | Speaker notes (design and final walkthrough) kept local only, removed from Git and git-ignored | User decision: they are recording scripts, not project documentation |

## Change log

- 2026-11-02: Project scaffold created.
- 2026-11-03: Plan agreed (Steps 0–7), scaffold pushed to GitHub `main`.
- 2026-11-03: Step 1 API exploration done; findings in docs/01_api_exploration/README.md.
- 2026-11-03: Step 2 design proposed in docs/02_design/README.md (approved the same day).
- 2026-11-03: PR #1 (Steps 1–2) merged. Architecture and data model redrawn in draw.io.
- 2026-11-03: Design walkthrough recorded by the user (kept local, not on GitHub); submitted to the SME.
- 2026-11-03: Session handoff; waiting for SME approval before Step 3.
- 2026-11-04: SME approved the design; Step 3 build started on branch `worktree-step-3-build`.
- 2026-11-04: Step 3 built in 11 stages (3.1–3.11), one commit each. Local simulated week (2026-03-17 → 23 + rerun of
  2026-03-20) reproduces Step 1: 39 events (8hunphufxp 11, 9k4tbcdfg0 28), 0 duplicated keys after the rerun.
- 2026-11-10: PR #5 merged. Step 4: CDK bootstrap, secret, `cdk deploy WistiaPipeline` (us-east-1), smoke test passed.
- 2026-11-10: Step 5: simulated week on AWS, 8 runs SUCCEEDED, 39 events, 0 duplicates after the rerun of 2026-03-20;
  evidence in docs/05_production_run/README.md.
- 2026-11-10: README results + requirements coverage (FR1–FR12) added; Step 7 started.
- 2026-11-10: Walkthrough speaker notes written (PRs #8–#10), then removed from the repo; kept locally.
