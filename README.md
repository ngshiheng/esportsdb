# esportsdb

[![Scrape (fast)](https://github.com/ngshiheng/esportsdb/actions/workflows/scrape-fast.yml/badge.svg)](https://github.com/ngshiheng/esportsdb/actions/workflows/scrape-fast.yml)
[![Scrape (slow)](https://github.com/ngshiheng/esportsdb/actions/workflows/scrape-slow.yml/badge.svg)](https://github.com/ngshiheng/esportsdb/actions/workflows/scrape-slow.yml)
[![Scrape (history)](https://github.com/ngshiheng/esportsdb/actions/workflows/scrape-history.yml/badge.svg)](https://github.com/ngshiheng/esportsdb/actions/workflows/scrape-history.yml)

A self-contained PandaScore scraper that builds and maintains a SQLite database of esports data (videogames, leagues, series, tournaments, matches, teams, players). The DB is persisted as a GitHub Actions artifact and kept fresh via a three-workflow CI pipeline.

## How it works

```mermaid
flowchart TD
    PS[("PandaScore API<br>api.pandascore.co")]

    subgraph CI ["GitHub Actions (serialised via concurrency group)"]
        S["scrape-slow.yml<br>Daily 02:00 UTC<br>all non-match tables"]
        F["scrape-fast.yml<br>Every 2 hours<br>upcoming / live matches + teams"]
        B["scrape-history.yml<br>workflow_dispatch only<br>full historical matches sweep"]
    end

    ART[("GitHub Artifact<br>esports.db")]
    RW["Railway<br>Datasette"]
    USER(["User"])

    PS -->|"paginated REST<br>100 records/page<br>4s inter-page delay"| S
    PS --> F
    PS --> B

    ART -->|"download at job start"| S
    ART -->|"download at job start"| F
    ART -->|"download at job start"| B

    S -->|"upload on success"| ART
    F -->|"upload on success"| ART
    B -->|"upload on success"| ART

    S -->|"redeploy"| RW
    F -->|"redeploy"| RW

    USER -->|"SQL / Datasette UI"| RW
```

All three workflows share the **`esportsdb-artifact` concurrency group** (`cancel-in-progress: false`). This acts as a mutex — only one job holds the artifact lock at a time; others queue and wait.

## CI Workflows

| Workflow             | Schedule                 | Resources                                                            | Purpose                                                                               |
| -------------------- | ------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `scrape-slow.yml`    | Daily 02:00 UTC          | `videogames`, `leagues`, `series`, `tournaments`, `teams`, `players` | Full daily rescrape of all non-match reference tables; Docker publish; Railway deploy |
| `scrape-fast.yml`    | Every 2 hours            | `*_upcoming`, `*_running`, `teams`                                   | Refresh upcoming/live matches and team data; Railway deploy                           |
| `scrape-history.yml` | `workflow_dispatch` only | `matches` (no filter)                                                | One-shot full historical matches backfill (~253K rows, ~2.5 h)                        |
| `test.yml`           | Every push / PR          | —                                                                    | Run unit tests                                                                        |

## Database Schema

```mermaid
erDiagram
    videogames {
        int id PK
        text name
        text slug
        text current_version
    }
    leagues {
        int id PK
        text name
        text slug
        int videogame_id FK
    }
    series {
        int id PK
        text full_name
        text season
        int year
        int league_id FK
        int videogame_id FK
    }
    tournaments {
        int id PK
        text name
        text tier
        int serie_id FK
        int league_id FK
        int videogame_id FK
    }
    matches {
        int id PK
        text status
        text match_type
        text scheduled_at
        int tournament_id FK
        int serie_id FK
        int league_id FK
        int videogame_id FK
    }
    match_opponents {
        int id PK
        int match_id FK
        int opponent_id
        text opponent_type
        int score
        int is_winner
    }
    teams {
        int id PK
        text name
        text acronym
        text location
        int current_videogame_id FK
    }
    players {
        int id PK
        text name
        text nationality
        text role
        int current_team_id FK
        int current_videogame_id FK
    }

    videogames ||--o{ leagues : ""
    videogames ||--o{ series : ""
    videogames ||--o{ tournaments : ""
    videogames ||--o{ matches : ""
    videogames ||--o{ teams : ""
    videogames ||--o{ players : ""
    leagues ||--o{ series : ""
    leagues ||--o{ tournaments : ""
    leagues ||--o{ matches : ""
    series ||--o{ tournaments : ""
    series ||--o{ matches : ""
    tournaments ||--o{ matches : ""
    matches ||--o{ match_opponents : ""
    teams ||--o{ players : ""
```

All tables use `INSERT OR REPLACE` upserts. `PRAGMA foreign_keys=ON` and `PRAGMA journal_mode=WAL` are set on every connection.

## FK Dependency Order

Resources must be scraped in dependency order within a run, or the parent tables must already exist in the DB from a prior run.

```
videogames → leagues → series → tournaments → matches → match_opponents
                                            ↗
                       teams → players
```

Sub-resources (e.g. `matches_upcoming`) share the same FK dependencies as their parent resource. They use `skip_fk_errors=True` — orphaned rows are logged and skipped rather than crashing the run.

## Rate Limiting & Caching

| Setting          | Value                      | Notes                                                                                                                                                                                   |
| ---------------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Inter-page delay | 5.0 s (3.0 s for backfill) | Keeps throughput ~720 req/hr vs 1,000/hr limit                                                                                                                                          |
| HTTP cache TTL   | None (no expiry)           | `hishel` SQLite-backed cache — entries persist until manually cleared; only HTTP 200 responses are stored (`_SuccessOnlyFilter`), so transient 5xx errors are never replayed from cache |
| Max retries      | 5                          | Exponential backoff on `httpx.RequestError`, HTTP 429, and HTTP 5xx                                                                                                                     |
| Backoff factor   | 2.0 s initial              | `tenacity` `wait_exponential`, min 2 s, max 60 s                                                                                                                                        |

## Secrets required

| Secret                   | Used by                                               |
| ------------------------ | ----------------------------------------------------- |
| `PANDASCORE_API_KEY`     | All scrape jobs                                       |
| `DOCKERHUB_TOKEN`        | `scrape-slow.yml` — Docker image publish              |
| `RAILWAY_TOKEN`          | `scrape-fast.yml`, `scrape-slow.yml` — Railway deploy |
| `RAILWAY_PROJECT_ID`     | `scrape-fast.yml`, `scrape-slow.yml` — Railway deploy |
| `RAILWAY_ENVIRONMENT_ID` | `scrape-fast.yml`, `scrape-slow.yml` — Railway deploy |
| `RAILWAY_SERVICE_ID`     | `scrape-fast.yml`, `scrape-slow.yml` — Railway deploy |
