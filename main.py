#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "httpx>=0.27",
# ]
# ///
"""
PandaScore periodic scraper — populates a local SQLite database.

Set PANDASCORE_API_KEY in your environment before running.

Usage:
    ./pandascore_scraper.py
    ./pandascore_scraper.py --db /data/pandascore.db
    ./pandascore_scraper.py --resources leagues,teams,players

Cron example (every 6 hours):
    0 */6 * * * PANDASCORE_API_KEY=sk-xxx /path/to/pandascore_scraper.py
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

import httpx

PANDASCORE_BASE_URL = "https://api.pandascore.co"
DEFAULT_PAGE_SIZE = 100
DEFAULT_DB_PATH = Path("pandascore.db")
HTTP_TOO_MANY_REQUESTS = 429
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2.0
INTER_PAGE_DELAY_SECONDS = 0.25

ALL_RESOURCES = (
    "videogames",
    "leagues",
    "series",
    "tournaments",
    "matches",
    "teams",
    "players",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScraperConfig:
    """Runtime settings for one scraper run.

    Attributes:
        api_key: PandaScore Bearer token, read from PANDASCORE_API_KEY.
        db_path: Filesystem path for the SQLite database.
        resources: Ordered tuple of resource names to scrape.
        page_size: Records requested per API page.
    """

    api_key: str
    db_path: Path
    resources: tuple[str, ...]
    page_size: int


@dataclass
class PandaScoreClient:
    """Fetches paginated resources from the PandaScore REST API.

    Attributes:
        api_key: Bearer token used for every request.
        page_size: Records per page.
    """

    api_key: str
    page_size: int = DEFAULT_PAGE_SIZE

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}

    def _page_params(self, page_number: int) -> dict[str, Any]:
        return {"page[number]": page_number, "page[size]": self.page_size, "sort": "id"}

    def _fetch_page_with_retry(
        self, endpoint: str, page_number: int
    ) -> list[dict[str, Any]]:
        url = f"{PANDASCORE_BASE_URL}/{endpoint}"
        backoff = INITIAL_BACKOFF_SECONDS

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = httpx.get(
                    url,
                    headers=self._auth_headers(),
                    params=self._page_params(page_number),
                    timeout=30.0,
                )
                if response.status_code == HTTP_TOO_MANY_REQUESTS:
                    log.warning(
                        "Rate limited on /%s page %d — backing off %.1fs.",
                        endpoint,
                        page_number,
                        backoff,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as exc:
                log.error("HTTP %d on /%s: %s", exc.response.status_code, endpoint, exc)
                raise

            except httpx.RequestError as exc:
                log.warning(
                    "Request error on /%s (attempt %d/%d): %s",
                    endpoint,
                    attempt,
                    MAX_RETRIES,
                    exc,
                )
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(backoff)
                backoff *= 2

        return []

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
            time.sleep(INTER_PAGE_DELAY_SECONDS)


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
            db.upsert(table, to_row(record))
            if extra_rows_fn:
                for extra_row in extra_rows_fn(record):
                    db.upsert("match_opponents", extra_row)
        db.commit()
        total += len(page)
    return total


RESOURCE_CONFIG: dict[str, dict[str, Any]] = {
    "videogames": {"table": "videogames", "to_row": videogame_to_row},
    "leagues": {"table": "leagues", "to_row": league_to_row},
    "series": {"table": "series", "to_row": series_to_row},
    "tournaments": {"table": "tournaments", "to_row": tournament_to_row},
    "matches": {
        "table": "matches",
        "to_row": match_to_row,
        "extra_rows_fn": match_opponent_rows,
    },
    "teams": {"table": "teams", "to_row": team_to_row},
    "players": {"table": "players", "to_row": player_to_row},
}


def run_scrape(config: ScraperConfig) -> None:
    client = PandaScoreClient(api_key=config.api_key, page_size=config.page_size)
    db = Database(path=config.db_path)
    db.ensure_schema()

    log.info("Starting scrape: %s", list(config.resources))
    started_at = time.monotonic()

    try:
        for resource in config.resources:
            cfg = RESOURCE_CONFIG.get(resource)
            if cfg is None:
                log.warning("Unknown resource '%s' — skipping.", resource)
                continue
            log.info("  Scraping /%s ...", resource)
            count = scrape_resource(client, db, endpoint=resource, **cfg)
            log.info("    → %d records upserted.", count)
    finally:
        db.close()

    log.info("Scrape complete in %.1fs.", time.monotonic() - started_at)


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

    return ScraperConfig(
        api_key=api_key,
        db_path=Path(args.db),
        resources=tuple(valid),
        page_size=args.page_size,
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

    run_scrape(_build_config(parser.parse_args()))


if __name__ == "__main__":
    main()
