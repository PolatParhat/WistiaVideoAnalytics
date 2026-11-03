# Wistia Video Analytics Pipeline

End-to-end data engineering project: ingest media and visitor analytics from the Wistia Stats API,
transform them with PySpark into a dimensional model on AWS, and report on video engagement across
Facebook and YouTube.

## Documents

| Step | Document |
|---|---|
| 0 | [Requirements (redacted)](docs/00_requirements/requirements.md) |
| 1 | [API exploration: endpoints, volume, findings](docs/01_api_exploration/README.md) |
| 2 | [Architecture and data model](docs/02_design/README.md) |
| 2 | [Design walkthrough speaker notes (SME recording)](docs/02_design/speaker_notes.md) |
| — | [Decisions and change log](docs/decisions.md) |

## Setup

1. Copy `.env.example` to `.env` and set `WISTIA_API_TOKEN`.
2. More steps are added as the project grows.
