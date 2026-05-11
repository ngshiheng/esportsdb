#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "backoff>=2.2",
#   "hishel>=1.2",
#   "httpx>=0.27",
# ]
# ///
"""
PandaScore periodic scraper — populates a local SQLite database.

Set PANDASCORE_API_KEY in your environment before running.

Usage:
    ./main.py
    ./main.py --db /data/esports.db
    ./main.py --resources leagues,teams,players

    # Incremental matches only (last 48 hours):
    ./main.py --resources matches --since 48h

    # Historical backfill (one-time, safe to Ctrl-C and re-run):
    ./main.py --resources matches

Cron examples:
    # Slow tables — full rescrape once daily (~304 req, ~20 min)
    0 2 * * *   PANDASCORE_API_KEY=sk-xxx ./main.py --resources videogames,leagues,series,tournaments,teams,players

    # Fast table — incremental every 2 hours (~5 req/run)
    0 */2 * * * PANDASCORE_API_KEY=sk-xxx ./main.py --resources matches --since 48h

Rate budget: default --page-delay of 4.0s keeps throughput at ~900 req/hr (limit: 1,000/hr).
HTTP responses are cached locally via hishel (TTL 2h) — crash-safe to re-run immediately.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generator

import backoff
import httpx
from hishel import SyncSqliteStorage
from hishel.httpx import SyncCacheClient

PANDASCORE_BASE_URL = "https://api.pandascore.co"
DEFAULT_PAGE_SIZE = 100
DEFAULT_DB_PATH = Path("data/esports.db")
HTTP_TOO_MANY_REQUESTS = 429
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2.0
INTER_PAGE_DELAY_SECONDS = 4.0  # keeps throughput ~900 req/hr, under the 1k/hr limit

ALL_RESOURCES = (
    "videogames",
    "leagues",
    "series",
    "series_upcoming",
    "series_running",
    "tournaments",
    "tournaments_upcoming",
    "tournaments_running",
    "matches",
    "matches_upcoming",
    "matches_running",
    "teams",
    "players",
)

# FK dependency graph: if you request a child resource without its parents in
# the same run, the parent tables must already be populated in the database.
# scrape-slow handles full historical rescrape; scrape-fast handles upcoming/running
# sub-endpoints and incremental matches.
RESOURCE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "leagues": ("videogames",),
    "series": ("videogames", "leagues"),
    "series_upcoming": ("videogames", "leagues"),
    "series_running": ("videogames", "leagues"),
    "tournaments": ("videogames", "leagues", "series"),
    "tournaments_upcoming": ("videogames", "leagues", "series"),
    "tournaments_running": ("videogames", "leagues", "series"),
    "matches": ("videogames", "leagues", "series", "tournaments"),
    "matches_upcoming": ("videogames", "leagues", "series", "tournaments"),
    "matches_running": ("videogames", "leagues", "series", "tournaments"),
    "players": ("videogames", "teams"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when PandaScore returns HTTP 429 so backoff can retry it."""


def _on_backoff(details: dict) -> None:
    exc = details["exception"]
    endpoint = details["args"][1]
    page_number = details["args"][2]
    wait = details["wait"]
    tries = details["tries"]
    if isinstance(exc, RateLimitError):
        log.warning(
            "Rate limited on /%s page %d (attempt %d) — backing off %.1fs.",
            endpoint,
            page_number,
            tries,
            wait,
        )
    else:
        log.warning(
            "Request error on /%s page %d (attempt %d) — backing off %.1fs: %s",
            endpoint,
            page_number,
            tries,
            wait,
            exc,
        )


@dataclass(frozen=True)
class ScraperConfig:
    """Runtime settings for one scraper run.

    Attributes:
        api_key: PandaScore Bearer token, read from PANDASCORE_API_KEY.
        db_path: Filesystem path for the SQLite database.
        resources: Ordered tuple of resource names to scrape.
        page_size: Records requested per API page.
        since: ISO-8601 datetime string; when set, matches are filtered to
               ``begin_at >= since``.  None means full history.
        page_delay: Seconds to sleep between paginated requests.
    """

    api_key: str
    db_path: Path
    resources: tuple[str, ...]
    page_size: int
    since: str | None = None
    page_delay: float = INTER_PAGE_DELAY_SECONDS


@dataclass
class PandaScoreClient:
    """Fetches paginated resources from the PandaScore REST API.

    Attributes:
        api_key: Bearer token used for every request.
        page_size: Records per page.
        since: Optional ISO-8601 lower-bound filter applied to ``matches``
               endpoint only (``filter[begin_at][gte]``).
        page_delay: Seconds to sleep between pages.
    """

    api_key: str
    page_size: int = DEFAULT_PAGE_SIZE
    since: str | None = None
    page_delay: float = INTER_PAGE_DELAY_SECONDS
    _http: SyncCacheClient = field(init=False, repr=False)

    def __post_init__(self) -> None:
        storage = SyncSqliteStorage(default_ttl=7200)
        self._http = SyncCacheClient(
            storage=storage,
            headers={"Authorization": f"Bearer {self.api_key}"},
        )

    def close(self) -> None:
        self._http.close()

    def _page_params(self, page_number: int, endpoint: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            "page[number]": page_number,
            "page[size]": self.page_size,
            "sort": "id",
        }
        if self.since and endpoint == "matches":
            params["filter[begin_at][gte]"] = self.since
        return params

    @backoff.on_exception(
        backoff.expo,
        (httpx.RequestError, RateLimitError),
        max_tries=MAX_RETRIES,
        factor=INITIAL_BACKOFF_SECONDS,
        jitter=None,
        on_backoff=_on_backoff,
        logger=None,
    )
    def _fetch_page_with_retry(
        self, endpoint: str, page_number: int
    ) -> list[dict[str, Any]]:
        url = f"{PANDASCORE_BASE_URL}/{endpoint}"
        response = self._http.get(
            url,
            params=self._page_params(page_number, endpoint),
            timeout=30.0,
        )
        if response.status_code == HTTP_TOO_MANY_REQUESTS:
            raise RateLimitError(f"429 on /{endpoint} page {page_number}")
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            log.error("HTTP %d on /%s: %s", exc.response.status_code, endpoint, exc)
            raise
        return response.json()

    def fetch_total(self, endpoint: str) -> int | None:
        """Return the total record count for an endpoint via X-Total header.

        Costs exactly one API request (page[size]=1).
        Returns None if the header is absent.
        """
        url = f"{PANDASCORE_BASE_URL}/{endpoint}"
        response = self._http.get(
            url,
            params={"page[number]": 1, "page[size]": 1, "sort": "id"},
            timeout=30.0,
        )
        response.raise_for_status()
        raw = response.headers.get("X-Total")
        return int(raw) if raw is not None else None

    def fetch_all(self, endpoint: str) -> Generator[list[dict[str, Any]], None, None]:
        """Yield one page at a time; caller commits after each batch."""
        page_number = 1
        while True:
            records = self._fetch_page_with_retry(endpoint, page_number)
            if not records:
                break
            yield records
            if len(records) < self.page_size:
                break
            page_number += 1
            time.sleep(self.page_delay)


SCHEMA_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS videogames (
        id               INTEGER PRIMARY KEY,
        name             TEXT    NOT NULL,
        slug             TEXT    NOT NULL,
        current_version  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS leagues (
        id              INTEGER PRIMARY KEY,
        name            TEXT    NOT NULL,
        slug            TEXT    NOT NULL,
        url             TEXT,
        image_url       TEXT,
        videogame_id    INTEGER REFERENCES videogames(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS series (
        id              INTEGER PRIMARY KEY,
        name            TEXT,
        full_name       TEXT,
        slug            TEXT,
        season          TEXT,
        year            INTEGER,
        begin_at        TEXT,
        end_at          TEXT,
        league_id       INTEGER REFERENCES leagues(id),
        videogame_id    INTEGER REFERENCES videogames(id),
        winner_id       INTEGER,
        winner_type     TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tournaments (
        id              INTEGER PRIMARY KEY,
        name            TEXT,
        full_name       TEXT,
        slug            TEXT,
        begin_at        TEXT,
        end_at          TEXT,
        serie_id        INTEGER REFERENCES series(id),
        league_id       INTEGER REFERENCES leagues(id),
        videogame_id    INTEGER REFERENCES videogames(id),
        tier            TEXT,
        has_bracket     INTEGER,
        live_supported  INTEGER,
        detailed_stats  INTEGER,
        prizepool       TEXT,
        winner_id       INTEGER,
        winner_type     TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS matches (
        id                      INTEGER PRIMARY KEY,
        name                    TEXT,
        slug                    TEXT,
        tournament_id           INTEGER REFERENCES tournaments(id),
        serie_id                INTEGER REFERENCES series(id),
        league_id               INTEGER REFERENCES leagues(id),
        videogame_id            INTEGER REFERENCES videogames(id),
        status                  TEXT,
        match_type              TEXT,
        number_of_games         INTEGER,
        scheduled_at            TEXT,
        begin_at                TEXT,
        end_at                  TEXT,
        winner_id               INTEGER,
        winner_type             TEXT,
        rescheduled             INTEGER,
        original_scheduled_at   TEXT,
        forfeit                 INTEGER,
        complete                INTEGER,
        detailed_stats          INTEGER,
        live_embed_url          TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS teams (
        id                      INTEGER PRIMARY KEY,
        name                    TEXT    NOT NULL,
        slug                    TEXT    NOT NULL,
        acronym                 TEXT,
        image_url               TEXT,
        location                TEXT,
        current_videogame_id    INTEGER REFERENCES videogames(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS players (
        id                      INTEGER PRIMARY KEY,
        name                    TEXT    NOT NULL,
        slug                    TEXT,
        first_name              TEXT,
        last_name               TEXT,
        image_url               TEXT,
        nationality             TEXT,
        role                    TEXT,
        birthday                TEXT,
        active                  INTEGER,
        current_team_id         INTEGER REFERENCES teams(id),
        current_videogame_id    INTEGER REFERENCES videogames(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS match_opponents (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id        INTEGER NOT NULL REFERENCES matches(id),
        opponent_id     INTEGER NOT NULL,
        opponent_type   TEXT    NOT NULL,
        score           INTEGER,
        is_winner       INTEGER,
        UNIQUE (match_id, opponent_id, opponent_type)
    )
    """,
)


@dataclass
class Database:
    """Manages the SQLite connection, schema, and upsert operations.

    Attributes:
        path: Filesystem path to the .db file.
    """

    path: Path
    _connection: sqlite3.Connection = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._connection = sqlite3.connect(self.path)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA foreign_keys=ON")

    def ensure_schema(self) -> None:
        for statement in SCHEMA_DDL:
            self._connection.execute(statement)
        self._connection.commit()

    def upsert(self, table: str, row: dict[str, Any]) -> None:
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" * len(row))
        self._connection.execute(
            f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})",
            list(row.values()),
        )

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()


def _nested_id(record: dict[str, Any], key: str) -> int | None:
    nested = record.get(key)
    return nested.get("id") if isinstance(nested, dict) else None


def videogame_to_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record["id"],
        "name": record["name"],
        "slug": record["slug"],
        "current_version": record.get("current_version"),
    }


def league_to_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record["id"],
        "name": record["name"],
        "slug": record["slug"],
        "url": record.get("url"),
        "image_url": record.get("image_url"),
        "videogame_id": _nested_id(record, "videogame"),
    }


def series_to_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record["id"],
        "name": record.get("name"),
        "full_name": record.get("full_name"),
        "slug": record.get("slug"),
        "season": record.get("season"),
        "year": record.get("year"),
        "begin_at": record.get("begin_at"),
        "end_at": record.get("end_at"),
        "league_id": _nested_id(record, "league"),
        "videogame_id": _nested_id(record, "videogame"),
        "winner_id": record.get("winner_id"),
        "winner_type": record.get("winner_type"),
    }


def tournament_to_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record["id"],
        "name": record.get("name"),
        "full_name": record.get("full_name"),
        "slug": record.get("slug"),
        "begin_at": record.get("begin_at"),
        "end_at": record.get("end_at"),
        "serie_id": _nested_id(record, "serie"),
        "league_id": _nested_id(record, "league"),
        "videogame_id": _nested_id(record, "videogame"),
        "tier": record.get("tier"),
        "has_bracket": record.get("has_bracket"),
        "live_supported": record.get("live_supported"),
        "detailed_stats": record.get("detailed_stats"),
        "prizepool": record.get("prizepool"),
        "winner_id": record.get("winner_id"),
        "winner_type": record.get("winner_type"),
    }


def match_to_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record["id"],
        "name": record.get("name"),
        "slug": record.get("slug"),
        "tournament_id": _nested_id(record, "tournament"),
        "serie_id": _nested_id(record, "serie"),
        "league_id": _nested_id(record, "league"),
        "videogame_id": _nested_id(record, "videogame"),
        "status": record.get("status"),
        "match_type": record.get("match_type"),
        "number_of_games": record.get("number_of_games"),
        "scheduled_at": record.get("scheduled_at"),
        "begin_at": record.get("begin_at"),
        "end_at": record.get("end_at"),
        "winner_id": record.get("winner_id"),
        "winner_type": record.get("winner_type"),
        "rescheduled": record.get("rescheduled"),
        "original_scheduled_at": record.get("original_scheduled_at"),
        "forfeit": record.get("forfeit"),
        "complete": record.get("complete"),
        "detailed_stats": record.get("detailed_stats"),
        "live_embed_url": record.get("live_embed_url"),
    }


def match_opponent_rows(match_record: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = match_record["id"]
    winner_id = match_record.get("winner_id")
    rows: list[dict[str, Any]] = []

    for slot in match_record.get("opponents", []):
        opponent = slot.get("opponent") or {}
        opponent_id = opponent.get("id")
        if opponent_id is None:
            continue
        rows.append(
            {
                "match_id": match_id,
                "opponent_id": opponent_id,
                "opponent_type": opponent.get("type") or slot.get("type", "Unknown"),
                "score": slot.get("score"),
                "is_winner": int(opponent_id == winner_id) if winner_id else None,
            }
        )

    return rows


def team_to_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record["id"],
        "name": record["name"],
        "slug": record["slug"],
        "acronym": record.get("acronym"),
        "image_url": record.get("image_url"),
        "location": record.get("location"),
        "current_videogame_id": _nested_id(record, "current_videogame"),
    }


def player_to_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record["id"],
        "name": record["name"],
        "slug": record.get("slug"),
        "first_name": record.get("first_name"),
        "last_name": record.get("last_name"),
        "image_url": record.get("image_url"),
        "nationality": record.get("nationality"),
        "role": record.get("role"),
        "birthday": record.get("birthday"),
        "active": record.get("active"),
        "current_team_id": _nested_id(record, "current_team"),
        "current_videogame_id": _nested_id(record, "current_videogame"),
    }


def scrape_resource(
    client: PandaScoreClient,
    db: Database,
    endpoint: str,
    to_row: Any,
    table: str,
    extra_rows_fn: Any = None,
    skip_fk_errors: bool = False,
) -> int:
    """Page through one endpoint, upsert rows, and commit after each page.

    Args:
        client: The API client.
        db: The database instance.
        endpoint: PandaScore endpoint path (e.g. 'matches').
        to_row: Function mapping one API record to a DB row dict.
        table: Target table name.
        extra_rows_fn: Optional function producing additional junction rows
                       from the same record (e.g. match_opponent_rows).
    """
    total = 0
    for page in client.fetch_all(endpoint):
        for record in page:
            row = to_row(record)
            try:
                db.upsert(table, row)
            except sqlite3.IntegrityError as exc:
                log.error(
                    "FK violation upserting into '%s' (record id=%s): %s\n  row: %s",
                    table,
                    record.get("id"),
                    exc,
                    row,
                )
                if skip_fk_errors:
                    continue
                raise
            if extra_rows_fn:
                for extra_row in extra_rows_fn(record):
                    try:
                        db.upsert("match_opponents", extra_row)
                    except sqlite3.IntegrityError as exc:
                        log.error(
                            "FK violation upserting into 'match_opponents' "
                            "(match_id=%s): %s\n  row: %s",
                            extra_row.get("match_id"),
                            exc,
                            extra_row,
                        )
                        if skip_fk_errors:
                            continue
                        raise
        db.commit()
        total += len(page)
    return total


RESOURCE_CONFIG: dict[str, dict[str, Any]] = {
    "videogames": {"table": "videogames", "to_row": videogame_to_row},
    "leagues": {"table": "leagues", "to_row": league_to_row},
    "series": {"table": "series", "to_row": series_to_row},
    "series_upcoming": {
        "endpoint": "series/upcoming",
        "table": "series",
        "to_row": series_to_row,
        "skip_fk_errors": True,
    },
    "series_running": {
        "endpoint": "series/running",
        "table": "series",
        "to_row": series_to_row,
        "skip_fk_errors": True,
    },
    "tournaments": {"table": "tournaments", "to_row": tournament_to_row},
    "tournaments_upcoming": {
        "endpoint": "tournaments/upcoming",
        "table": "tournaments",
        "to_row": tournament_to_row,
        "skip_fk_errors": True,
    },
    "tournaments_running": {
        "endpoint": "tournaments/running",
        "table": "tournaments",
        "to_row": tournament_to_row,
        "skip_fk_errors": True,
    },
    "matches": {
        "table": "matches",
        "to_row": match_to_row,
        "extra_rows_fn": match_opponent_rows,
        "skip_fk_errors": True,
    },
    "matches_upcoming": {
        "endpoint": "matches/upcoming",
        "table": "matches",
        "to_row": match_to_row,
        "extra_rows_fn": match_opponent_rows,
        "skip_fk_errors": True,
    },
    "matches_running": {
        "endpoint": "matches/running",
        "table": "matches",
        "to_row": match_to_row,
        "extra_rows_fn": match_opponent_rows,
        "skip_fk_errors": True,
    },
    "teams": {"table": "teams", "to_row": team_to_row},
    "players": {"table": "players", "to_row": player_to_row},
}


def run_scrape(config: ScraperConfig) -> None:
    client = PandaScoreClient(
        api_key=config.api_key,
        page_size=config.page_size,
        since=config.since,
        page_delay=config.page_delay,
    )
    db = Database(path=config.db_path)
    db.ensure_schema()

    log.info(
        "Starting scrape: %s%s",
        list(config.resources),
        f" (since {config.since})" if config.since else "",
    )
    started_at = time.monotonic()

    try:
        for resource in config.resources:
            cfg = RESOURCE_CONFIG.get(resource)
            if cfg is None:
                log.warning("Unknown resource '%s' — skipping.", resource)
                continue
            cfg = dict(cfg)  # copy — avoid mutating the module-level dict
            endpoint = cfg.pop("endpoint", resource)
            log.info("  Scraping /%s ...", endpoint)
            count = scrape_resource(client, db, endpoint=endpoint, **cfg)
            log.info("    → %d records upserted.", count)
    finally:
        client.close()
        db.close()

    log.info("Scrape complete in %.1fs.", time.monotonic() - started_at)


def _parse_since(value: str) -> str:
    """Convert a human-friendly shorthand (e.g. ``48h``, ``7d``) to an
    ISO-8601 UTC datetime string, or return the value unchanged if it is
    already an ISO-8601 string."""
    import re
    from datetime import datetime, timedelta, timezone

    m = re.fullmatch(r"(\d+)([hd])", value.strip())
    if m:
        amount, unit = int(m.group(1)), m.group(2)
        delta = timedelta(hours=amount) if unit == "h" else timedelta(days=amount)
        return (datetime.now(timezone.utc) - delta).strftime("%Y-%m-%dT%H:%M:%SZ")
    return value


def _build_config(args: argparse.Namespace) -> ScraperConfig:
    api_key = os.environ.get("PANDASCORE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Error: PANDASCORE_API_KEY environment variable is not set.")

    requested = [r.strip() for r in args.resources.split(",") if r.strip()]
    unknown = set(requested) - set(ALL_RESOURCES)
    if unknown:
        log.warning("Unrecognised resources will be skipped: %s", unknown)

    valid = [r for r in requested if r in ALL_RESOURCES]
    if not valid:
        raise SystemExit("Error: no valid resources specified.")

    # Warn when child resources are requested without their parents in this run.
    # Parent tables must already be populated in the DB (e.g. from a prior slow scrape).
    requested_set = set(valid)
    for resource in valid:
        missing_parents = [
            p for p in RESOURCE_DEPENDENCIES.get(resource, ()) if p not in requested_set
        ]
        if missing_parents:
            log.warning(
                "Resource '%s' has FK dependencies on %s which are NOT in this run. "
                "Those tables must already be populated in the database, "
                "otherwise you will hit FOREIGN KEY constraint errors.",
                resource,
                missing_parents,
            )

    since: str | None = None
    if args.since:
        since = _parse_since(args.since)
        log.info("Incremental mode: filtering matches to begin_at >= %s", since)

    return ScraperConfig(
        api_key=api_key,
        db_path=Path(args.db),
        resources=tuple(valid),
        page_size=args.page_size,
        since=since,
        page_delay=args.page_delay,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape PandaScore API into SQLite. Reads PANDASCORE_API_KEY from env.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        metavar="PATH",
        help="SQLite database file path.",
    )
    parser.add_argument(
        "--resources",
        default=",".join(ALL_RESOURCES),
        metavar="LIST",
        help=f"Comma-separated resources. Options: {', '.join(ALL_RESOURCES)}",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        metavar="N",
        help="Records per API page.",
    )
    parser.add_argument(
        "--since",
        default=None,
        metavar="WHEN",
        help=(
            "Only fetch matches with begin_at >= WHEN. "
            "Accepts ISO-8601 (2025-01-01T00:00:00Z) or shorthand: 48h, 7d. "
            "Ignored for non-match resources."
        ),
    )
    parser.add_argument(
        "--page-delay",
        type=float,
        default=INTER_PAGE_DELAY_SECONDS,
        metavar="SECS",
        help="Seconds between paginated requests. Default keeps throughput ~900 req/hr.",
    )
    parser.add_argument(
        "--count",
        action="store_true",
        help=(
            "Print the total record count for each requested resource "
            "(1 API request per resource) then exit. Does not scrape."
        ),
    )

    args = parser.parse_args()

    if args.count:
        api_key = os.environ.get("PANDASCORE_API_KEY", "").strip()
        if not api_key:
            raise SystemExit(
                "Error: PANDASCORE_API_KEY environment variable is not set."
            )
        client = PandaScoreClient(api_key=api_key)
        requested = [r.strip() for r in args.resources.split(",") if r.strip()]
        try:
            for resource in requested:
                cfg = RESOURCE_CONFIG.get(resource)
                if cfg is None:
                    print(f"{resource}: unknown resource")
                    continue
                endpoint = cfg.get("endpoint", resource)
                total = client.fetch_total(endpoint)
                pages = ((total - 1) // DEFAULT_PAGE_SIZE + 1) if total else "?"
                delay_min = (
                    (pages if isinstance(pages, int) else 0) * INTER_PAGE_DELAY_SECONDS
                ) / 60
                print(
                    f"{endpoint}: {total:,} records  "
                    f"~{pages} pages  "
                    f"~{delay_min:.1f} min delay"
                )
        finally:
            client.close()
        return

    run_scrape(_build_config(args))


if __name__ == "__main__":
    main()
