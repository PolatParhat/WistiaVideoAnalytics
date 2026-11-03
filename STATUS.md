# Status: Wistia Video Analytics Pipeline
Updated: 2026-11-03 by /handoff

## Where we are
Steps 0–2 done. The design (`docs/02_design/README.md`, draw.io diagrams) is merged to main and was submitted to the
SME with a recorded walkthrough (video kept local). **Waiting for SME approval; Step 3 (build) does not start until
the user reports it.** PR #4 (this handoff + video decision) is open.

## Done this session
- Scaffold + plan (506cd47, 58d558f); repo https://github.com/PolatParhat/WistiaVideoAnalytics (public).
- Step 1 API exploration (59a61f6): Bearer and Basic both work; pagination ends on `[]`; date filters inclusive;
  1,242 events (8hunphufxp 916, 9k4tbcdfg0 326); `event_key` not unique; no channel field. Masked samples committed.
- Step 2 design (9041f13, 2260a94), redrawn in draw.io (7c97c89, PR #2), SME speaker notes (c87d36d, PR #3).
- Decision: walkthrough videos never go on GitHub (49543f9, PR #4).

## Next actions
1. User merges PR #4: `gh pr merge 4 --merge`, then updates the local main folder:
   `cd ~/Documents/DataEngineerBootcamp/Projects/WistiaVideoAnalytics && git pull`.
2. When the SME answers: apply feedback to `docs/02_design/README.md` + `docs/decisions.md` (channel mapping is the
   most likely change; it becomes `config/media.yaml` in Step 3).
3. After approval: Step 3, stage 3.1, single-file `ingest.py` (one API call, prints the result) on a new branch.
   Follow the build order in design doc section 5, one commit per stage.

## Open items
- PR #4 open (branch `worktree-step-2-video-link`).
- SME approval pending; the speaker notes asked the SME to confirm 4 points: play_rate moved to `fact_media_daily`,
  channel mapping (9k4tbcdfg0 = Facebook, 8hunphufxp = YouTube), Bearer auth, simulated 7-day run.
- Merged remote branches that can be deleted (user only; blocked for Claude): `worktree-step-1-plan-api-exploration`,
  `worktree-step-2-drawio-diagram`, `worktree-step-2-sme-speaker-notes` (and `worktree-step-2-video-link` after merge).

## Environment gotchas
- The SessionStart hook reads STATUS.md from the **main folder**: after merging a PR, run `git pull` there or the
  next session starts from stale state.
- `.env` (token) lives in the main project folder only, not in worktrees; scripts read it from there.
- Export diagrams: `/Applications/draw.io.app/Contents/MacOS/draw.io -x -f png -s 2 -b 20 -o <out>.png <in>.drawio`
- Video (local, ignored): `docs/02_design/DesignExplinationVideo.mov` in the main folder. Never upload to GitHub.
- Claude is blocked from deleting remote branches and creating public releases; the user runs those.

## Key facts to keep straight
- Media IDs in scope: `8hunphufxp`, `9k4tbcdfg0` (the requirements' example ID `v08dlrgr7v` is not in scope).
- No dbt; AWS only; Python ingestion; PySpark transforms; GitHub Actions CI/CD.
- FR8 = simulated 7-day backfill on the real AWS pipeline, `run_date` 2026-03-17 → 23 (not 7 calendar days).
- Simulation week events per day: 8hunphufxp [0,1,1,3,1,3,2] = 11, 9k4tbcdfg0 [9,4,5,2,2,0,6] = 28 (total 39).
- Event natural key = `(event_key, visitor_key, received_at)`.
- Build style: single script first → common library → tests → cloud; one commit per stage (user's request).
- The GitHub repo is PUBLIC: the token goes only in `.env` / Secrets Manager / GitHub secrets, never in Git, chat or logs.
