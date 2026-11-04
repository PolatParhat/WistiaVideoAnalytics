# Wistia Video Analytics Pipeline

Learning project built to production standard, following the requirements in
`docs/00_requirements/requirements.md` (redacted copy of the .docx, which stays local because it holds the API token).

State: STATUS.md · History: docs/decisions.md (read it before re-deciding anything)

## Project context

- **Goal:** Ingest media-level and visitor-level analytics from the Wistia Stats API daily, transform them
  into a dimensional model, and report on video engagement (Facebook / YouTube) for the marketing team.
- **Constraints (from requirements):** AWS (or Azure) services only; **no dbt**; Python for API ingestion;
  **PySpark** for transformation; GitHub for version control and CI/CD (GitHub Actions); architecture must be
  presented to the SME for approval; FR8 "7 consecutive days in production" is met by a **simulated
  7-day backfill** on the real AWS pipeline (user decision 2026-11-03, see docs/decisions.md).
- **Scope:** media IDs `8hunphufxp` and `9k4tbcdfg0` only.
- **Build style:** grow code like a real engineer: one working script first, then move reusable parts into a
  common library, then tests, CI, infra. One commit per stage so the history shows the evolution.
- **Required behaviour:** token auth, pagination (all pages), incremental pulls on created_at/updated_at,
  error handling, retries, logging.
- **Target model (simplified, from requirements):** `dim_media`, `dim_visitor`, `fact_media_engagement`.
- **Deliverables:** GitHub repo with README, architecture diagram, setup instructions, pipeline code, CI/CD;
  optional dashboard; recorded walkthrough.

## Plan status

| Step | Description | Status |
|---|---|---|
| 0 | Project scaffold | Done |
| 1 | API exploration: auth, endpoints, fields, pagination, date filters (read-only calls) | Done (`docs/01_api_exploration/`) |
| 2 | Architecture + data model, one-page design + diagram (user approves; acts as SME) | Done (approved 2026-11-03; SME approved 2026-11-04, `docs/02_design/`) |
| 3 | Build: Python ingestion, PySpark transforms, DQ checks, tests, GitHub Actions CI/CD, AWS IaC | Next |
| 4 | Deploy to AWS (user runs the commands) | Not started |
| 5 | Simulated 7-day production run: backfill script, 7 runs with `run_date`, run-log evidence | Not started |
| 6 | Dashboard (optional FR11) | Not started |
| 7 | README, walkthrough video, AWS teardown; then hand-typing tutorial in a separate practice folder | Not started |

Timeline: finish in a couple of days. After the design is approved Claude builds everything; then
explains it so the user retypes it by hand (separate practice folder, never the production repo).

## Repo layout

```
WistiaVideoAnalytics/
├── CLAUDE.md · STATUS.md · README.md   rules · current state · reviewer-facing index
├── .env / .env.example                 secrets (git-ignored) / template
└── docs/
    ├── 00_requirements/                requirements.md (redacted, in Git); original .docx (local only)
    └── decisions.md                    decisions and change log
```

## Conventions

- **Secrets:** the Wistia token lives only in `.env` locally and in AWS Secrets Manager / GitHub secrets
  later. Never print it, hard-code it, or commit it.
- Raw API responses are immutable; fix data downstream. Column names lowercase snake_case.
- Jobs do I/O; transformations are pure functions with unit tests.
- Code changes on a branch (worktree) → CI green → PR → user merges. Never push to main.
- The user runs commands that create, change or delete cloud resources; Claude explains them.
