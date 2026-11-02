# Status: Wistia Video Analytics Pipeline
Updated: 2026-11-02 by /new-project

## Where we are
Step 0, project created. Scaffold committed; the project plan has been proposed but not agreed yet.

## Done this session
- Created the scaffold (CLAUDE.md, STATUS.md, README.md, docs/decisions.md, .gitignore), `git init`.
- Moved the requirements .docx to `docs/00_requirements/` (git-ignored: it contains the API token) and
  wrote a redacted text copy `docs/00_requirements/requirements.md`.
- Put the Wistia token in `.env` (git-ignored); `.env.example` is the template.

## Next actions
1. Agree on the project steps (proposed in chat), then write them into CLAUDE.md "Plan status" and here.
2. Decide whether to create the GitHub repo now (`gh repo create`).

## Open items
- The requirements contradict themselves on auth: FR2 says "Basic Auth", the example uses `Authorization: Bearer`.
  Check both in Step 1.
- The example snippet uses media ID `v08dlrgr7v`, but the in-scope IDs are `8hunphufxp` and `9k4tbcdfg0`.

## Environment gotchas
- (none yet)

## Key facts to keep straight
- Media IDs in scope: `8hunphufxp`, `9k4tbcdfg0`.
- No dbt; AWS/Azure only; Python ingestion; PySpark transforms; GitHub Actions CI/CD; 7-day production run.
- The token is a secret: `.env` only, never in Git, chat output or logs.
