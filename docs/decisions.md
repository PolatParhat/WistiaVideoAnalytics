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

## Change log

- 2026-11-02: Project scaffold created.
- 2026-11-03: Plan agreed (Steps 0–7), scaffold pushed to GitHub `main`.
- 2026-11-03: Step 1 API exploration done; findings in docs/01_api_exploration/README.md.
