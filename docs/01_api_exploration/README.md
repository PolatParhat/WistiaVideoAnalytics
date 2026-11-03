# Step 1: Wistia API exploration

Date: 2026-11-03. All calls were read-only `GET` requests to `https://api.wistia.com/v1/`.
Masked sample responses (IPs cut to two octets, tokens removed) are in [`samples/`](samples/).

## 1. Authentication (FR2)

| Method | Header | Result |
|---|---|---|
| Bearer | `Authorization: Bearer <token>` | 200 OK |
| Basic | `Authorization: Basic base64("api:<token>")` | 200 OK |

Both work. The requirement says "token-based Basic Auth"; Wistia's current docs use Bearer.
**Proposal:** use Bearer, the documented method; the client keeps the header in one place, so it is a one-line switch.
No rate-limit headers were returned. Wistia documents a limit of 600 requests per minute; we use about 20 per run.

## 2. Endpoints we need

| Endpoint | What it gives | Paginated | Date filter | Used for |
|---|---|---|---|---|
| `GET /medias/{id}.json` | Metadata: `hashed_id`, `name`, `duration`, `created`, `updated`, `project`, `share_link` | no | no | `dim_media` (FR3) |
| `GET /stats/medias/{id}.json` | Lifetime totals: `load_count`, `play_count`, `play_rate`, `hours_watched`, `engagement`, `visitors` | no | no | Reconciliation check |
| `GET /stats/medias/{id}/by_date.json?start_date&end_date` | **Daily** `load_count`, `play_count`, `hours_watched`, one row per day (zeros included) | no | **yes** | Daily media metrics (FR4) |
| `GET /stats/events.json?media_id&start_date&end_date&page&per_page` | One row per viewing session: `event_key`, `visitor_key`, `received_at`, `ip`, `country`, `region`, `city`, `percent_viewed`, `embed_url`, `user_agent_details` | **yes** | **yes** | `dim_visitor` + `fact_media_engagement` (FR5) |
| `GET /stats/visitors.json?page&per_page` | Visitor profile: `visitor_key`, `created_at`, `last_active_at`, lifetime `load_count`/`play_count`, browser | yes | **no** | Not used (see finding 4) |

## 3. Data volume (full history, both media)

| Media | Name | Created | Duration | Lifetime loads / plays (stats) | Events | Event date range |
|---|---|---|---|---|---|---|
| `8hunphufxp` | rivas_-_de_testimonial (1080p) (1) | 2024-06-10 | 988.7 s | 128,043 / 975 | 916 (10 pages) | 2024-11-03 → 2026-10-31 |
| `9k4tbcdfg0` | The Gap Method | 2025-01-04 | 2,195.0 s | 939 / 328 | 326 (4 pages) | 2025-01-04 → 2026-07-26 |

- Small data: about 1–3 events per day in total. A full history pull takes about 27 s (14 pages).
- Full history pull of both media: 1,242 events; no null `ip`, `country`, `visitor_key` or `received_at`; `email` is always null;
  `embed_url` is empty in 215 events; `percent_viewed` ranges 0.0–1.0 (76 events are exactly 0).

## 4. Findings that shape the design

1. **Pagination:** `page` + `per_page` (max 100). The end of the data is an **empty list `[]`**; there is no total count or
   "next" link, so the client loops until an empty page. Events are sorted newest first.
2. **Date filters work and are inclusive:** `start_date=2026-10-01&end_date=2026-10-07` returned 7 events for `8hunphufxp`,
   exactly the events whose UTC date is in 1–7 Oct (including `2026-10-07T00:27Z`). This makes the incremental pull and the
   7-day backfill (one `run_date` per run) possible.
3. **`event_key` is not unique.** In each media, two different events (different visitor, different time) share one
   `event_key`. The natural key must be `(event_key, visitor_key, received_at)`; deduplicating on `event_key` alone would
   silently drop real events.
4. **`/stats/visitors` is account-wide, cannot be filtered by media or date, and has no IP or country.** The required
   `dim_visitor` fields (`visitor_id`, `ip_address`, `country`) all come from the events. Proposal: build `dim_visitor` from events.
5. **No "channel" field exists.** Neither media has tags or a channel. `embed_url` shows the page where the video played:
   mostly `dataengineerinterviews.com`; Facebook/Meta ad clicks carry `fbclid`, `fbc_id` or `utm_source=meta`.
   No YouTube signal was found. Decision needed in Step 2: channel per media from a config mapping (assumption) and/or a
   per-event traffic source derived from `embed_url`.
6. **Recent activity is low.** `9k4tbcdfg0` has had no events since 2026-07-26. The busiest recent 7 days with events every day
   are 2026-10-12 → 2026-10-18 (13 events); the busiest 7 days overall with at least 3 events per day are
   2026-03-17 → 2026-03-23 (39 events: `8hunphufxp` 11, `9k4tbcdfg0` 28; each media has one day with 0). Candidate windows for the simulated 7-day run.
7. **Media metadata changes rarely** (`updated` 2026-06-29 for both). Re-pulling the 2 media records every run is cheapest;
   incremental logic applies to events and daily stats (FR7).
8. **Personal data:** events contain IP addresses and city-level location. Raw data stays in a private, encrypted bucket;
   only masked samples are in this public repo.

## 5. Open questions for Step 2

- Bearer (documented) vs Basic (named in FR2): proposal Bearer.
- Channel source: config mapping per media vs traffic source from `embed_url`.
- Fact table grain and metrics: per media × visitor × day from events (`play_count` = events, `total_watch_time` =
  Σ `percent_viewed` × `duration`, `watched_percent` = max/avg `percent_viewed`); `play_rate` needs loads, which exist only
  per media per day (`by_date`), not per visitor.
- 7-day window for the simulation: 2026-03-17 → 2026-03-23 (busy, both media) or 2026-10-12 → 2026-10-18 (recent).
