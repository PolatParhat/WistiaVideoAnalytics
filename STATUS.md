# Status: Wistia Video Analytics Pipeline
Updated: 2026-11-03 (draw.io diagrams)

## Where we are
Steps 1–2 done and merged (PR #1). Diagrams redrawn in draw.io on branch `worktree-step-2-drawio-diagram`
(worktree `.claude/worktrees/step-2-drawio-diagram`), PR pending. Plan agreed: Steps 0–7 in CLAUDE.md.

## Done this session
- Scaffold created and pushed to GitHub `main` (public repo PolatParhat/WistiaVideoAnalytics); commit 506cd47.
- Step 1: both Bearer and Basic auth work; endpoints, pagination (ends on `[]`), inclusive date filters, volume
  (1,242 events total), duplicate `event_key`, no channel field. Masked samples committed.
- Requirements explained to the user; plan agreed; decisions logged (simulated 7-day run, user = SME, build-first-then-teach).

## Next actions
1. User reviews the draw.io diagrams PR and merges.
2. Step 3: build in stages 3.1–3.11 (design doc section 5), one commit per stage, on a new branch → PR.

## Open items
- Step 2 choices made by the user (all recommended): Bearer, channel from config, fact grain + `fact_media_daily`,
  simulation window 2026-03-17 → 23. Diagrams: `docs/02_design/*.drawio` (source) + `*.png` (export).
- The example snippet uses media ID `v08dlrgr7v`; in-scope IDs are `8hunphufxp` and `9k4tbcdfg0`.

## Environment gotchas
- Export diagrams: `/Applications/draw.io.app/Contents/MacOS/draw.io -x -f png -s 2 -b 20 -o <out>.png <in>.drawio`
- Old remote branch `worktree-step-1-plan-api-exploration` (merged) still on GitHub; deleting it needs the user.
- `.env` (token) lives in the main project folder only, not in worktrees; scripts read it from there.

## Key facts to keep straight
- Media IDs in scope: `8hunphufxp`, `9k4tbcdfg0`.
- No dbt; AWS only; Python ingestion; PySpark transforms; GitHub Actions CI/CD.
- FR8 = simulated 7-day backfill on the real AWS pipeline (not 7 calendar days).
- The GitHub repo is PUBLIC: the token goes only in `.env` / Secrets Manager / GitHub secrets, never in Git, chat output or logs.
