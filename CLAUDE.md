# Wistia Video Analytics Pipeline

Learning project built to production standard, following the requirements in
`docs/00_requirements/requirements.md` (redacted copy of the .docx, which stays local because it holds the API token).

State: STATUS.md · History: docs/decisions.md (read it before re-deciding anything)

## Project context

- **Goal:** Ingest media-level and visitor-level analytics from the Wistia Stats API daily, transform them
  into a dimensional model, and report on video engagement (Facebook / YouTube) for the marketing team.
- **Constraints (from requirements):** AWS (or Azure) services only; **no dbt**; Python for API ingestion;
  **PySpark** for transformation; GitHub for version control and CI/CD (GitHub Actions); architecture must be
  presented to the SME for approval; pipeline runs in production mode for **7 consecutive days**.
- **Scope:** media IDs `8hunphufxp` and `9k4tbcdfg0` only.
- **Required behaviour:** token auth, pagination (all pages), incremental pulls on created_at/updated_at,
  error handling, retries, logging.
- **Target model (simplified, from requirements):** `dim_media`, `dim_visitor`, `fact_media_engagement`.
- **Deliverables:** GitHub repo with README, architecture diagram, setup instructions, pipeline code, CI/CD;
  optional dashboard; recorded walkthrough.

## Plan status

| Step | Description | Status |
|---|---|---|
| 0 | Project scaffold | Done |
| (plan to be agreed) | | |

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
