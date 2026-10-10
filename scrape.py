#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "hishel>=1.2",
#   "httpx>=0.27",
#   "tenacity>=9.0",
#   "typer>=0.12",
# ]
# ///
"""
PandaScore periodic scraper - populates a local SQLite database.

Set PANDASCORE_API_KEY in your environment before running.

Usage:
    make scrape-fast          # every-2h refresh: live/upcoming + last 48 h of matches
    make scrape-slow          # daily refresh: reference data (leagues, series, tournaments, teams, players)
    make scrape-history       # one-time full historical match backfill (run manually once)

    # Or invoke directly:
    ./scrape.py --resource matches_upcoming --resource matches_running

    # Incremental matches only (last 48 hours):
    ./scrape.py --resource matches --since 48h

    # Print record counts without scraping:
    ./scrape.py --count

Rate budget: default --page-delay of 5.0s keeps throughput at ~720 req/hr (limit: 1,000/hr).
HTTP responses are cached locally via hishel (TTL 2h) - crash-safe to re-run immediately.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Callable, Iterator

import httpx
import typer
from hishel import BaseFilter, FilterPolicy, SyncSqliteStorage
from hishel._policies import Response as HishelResponse
from hishel.httpx import SyncCacheClient
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

PANDASCORE_BASE_URL = "https://api.pandascore.co"
DEFAULT_PAGE_SIZE = 100
DEFAULT_DB_PATH = Path("data/esports.db")
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2.0
INTER_PAGE_DELAY_SECONDS = (
    5.0  # keeps throughput ~720 req/hr, well under the 1k/hr limit
)


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

app = typer.Typer(invoke_without_command=True)


class RateLimitError(Exception):
    """Raised when PandaScore returns HTTP 429 so tenacity can retry it."""


class ServerError(Exception):
    """Raised when PandaScore returns a 5xx so tenacity can retry it."""


class _SuccessOnlyFilter(BaseFilter[HishelResponse]):
    """Only allow HTTP 200 responses into the cache.

    Without this, transient 5xx responses would be stored and served on every
    retry, making tenacity retry against a cached error instead of the real API.
    """

    def needs_body(self) -> bool:
        return False

    def apply(self, item: HishelResponse, body: bytes | None) -> bool:
        return item.status_code == 200


def _before_sleep(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
    args = retry_state.args
    endpoint = args[1] if len(args) > 1 else "?"
    page_number = args[2] if len(args) > 2 else "?"
    wait = retry_state.upcoming_sleep
    tries = retry_state.attempt_number

    if isinstance(exc, RateLimitError):
        log.warning(
            "Rate limited on /%s page %d (attempt %d) - backing off %.1fs.",
            endpoint,
            page_number,
            tries,
            wait,
        )

    else:
        log.warning(
            "Request error on /%s page %d (attempt %d) - backing off %.1fs: %s",
            endpoint,
            page_number,
            tries,
            wait,
            exc,
        )


@dataclass(frozen=True)
class PageResult:
    """Page result from scraping"""

    records: list[dict[str, Any]]
    from_cache: bool


@dataclass(frozen=True)
class ScrapeResult:
    """Outcome for one resource scrape."""

    attempted: int = 0
    persisted: int = 0
    fk_rejected: int = 0
    api_incomplete: bool = False
    unresolved_relationships: int = 0
    missing_opponents: int = 0
    unsupported_opponents: int = 0
    records_to_retry: tuple[dict[str, Any], ...] = ()


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
        # default_ttl=None: entries never expire by TTL.
        # FilterPolicy: bypasses HTTP spec checks (PandaScore sends
        # Cache-Control: no-store) so responses are cached and served
        # regardless of server expiration directives.
        # _SuccessOnlyFilter: prevents error responses (5xx, 429) from being
        # stored in the cache - retries must always hit the real API.
        storage = SyncSqliteStorage(default_ttl=None)
        policy = FilterPolicy(response_filters=[_SuccessOnlyFilter()])
        self._http = SyncCacheClient(
            storage=storage,
            policy=policy,
            headers={"Authorization": f"Bearer {self.api_key}"},
        )

    def close(self) -> None:
        """Close the underlying HTTP client.

        Called automatically by ``contextlib.closing()`` when used as:
        ``with closing(PandaScoreClient(...)) as client``.
        """
        self._http.close()

    def _page_params(self, page_number: int, endpoint: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            "page[number]": page_number,
            "page[size]": self.page_size,
            "sort": "-begin_at" if (self.since and endpoint == "matches") else "id",
        }
        return params

    @retry(
        retry=retry_if_exception_type(
            (httpx.RequestError, RateLimitError, ServerError)
        ),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=INITIAL_BACKOFF_SECONDS, max=60),
        before_sleep=_before_sleep,
        reraise=True,
    )
    def _fetch_page_with_retry(self, endpoint: str, page_number: int) -> PageResult:
        url = f"{PANDASCORE_BASE_URL}/{endpoint}"
        response = self._http.get(
            url,
            params=self._page_params(page_number, endpoint),
        )
        if response.status_code == 429:
            raise RateLimitError(f"429 on /{endpoint} page {page_number}")

        if response.status_code >= 500:
            log.error(
                "HTTP %d on /%s page %d", response.status_code, endpoint, page_number
            )
            raise ServerError(
                f"{response.status_code} on /{endpoint} page {page_number}"
            )

        try:
            response.raise_for_status()

        except httpx.HTTPStatusError as exc:
            log.error("HTTP %d on /%s: %s", exc.response.status_code, endpoint, exc)
            raise

        from_cache = bool(response.extensions.get("hishel_from_cache"))
        if from_cache:
            log.info("/%s page %d served from cache", endpoint, page_number)

        return PageResult(records=response.json(), from_cache=from_cache)

    def fetch_total(self, endpoint: str) -> int | None:
        """Return the total record count for an endpoint via X-Total header.

        Costs exactly one API request (page[size]=1).
        Returns None if the header is absent.
        """
        url = f"{PANDASCORE_BASE_URL}/{endpoint}"
        response = self._http.get(
            url,
            params={"page[number]": 1, "page[size]": 1, "sort": "id"},
        )
        response.raise_for_status()
        raw = response.headers.get("X-Total")

        from_cache = bool(response.extensions.get("hishel_from_cache"))
        if from_cache:
            log.info("/%s served from cache", endpoint)

        return int(raw) if raw is not None else None

    def fetch_all(self, endpoint: str) -> Iterator[list[dict[str, Any]]]:
        """Yield one page at a time; caller commits after each batch."""
        page_number = 1

        while True:
            result = self._fetch_page_with_retry(endpoint, page_number)
            if not result.records:
                break

            if self.since and endpoint == "matches":
                # Sort is -begin_at (newest first); stop once records go before cutoff
                filtered = [
                    r
                    for r in result.records
                    if r.get("begin_at") and r["begin_at"] >= self.since
                ]
                if filtered:
                    yield filtered
                if len(filtered) < len(result.records):
                    break
            else:
                yield result.records

            if len(result.records) < self.page_size:
                break
            page_number += 1

            # Cache hits are instant - no need to throttle against the API rate limit.
            if not result.from_cache:
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
    "CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status)",
    "CREATE INDEX IF NOT EXISTS idx_matches_status_end_at ON matches(status, end_at)",
)


@dataclass
class Database:
    """Manages the SQLite connection, schema, and upsert operations."""

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
        columns = tuple(row)
        column_names = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        values = tuple(row.values())
        if table == "match_opponents":
            conflict_columns = ("match_id", "opponent_id", "opponent_type")
        else:
            conflict_columns = ("id",)
        conflict_target = ", ".join(conflict_columns)
        updates = tuple(column for column in columns if column not in conflict_columns)
        if updates:
            update_clause = ", ".join(
                f"{column} = excluded.{column}" for column in updates
            )
            sql = (
                f"INSERT INTO {table} ({column_names}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_target}) DO UPDATE SET {update_clause}"
            )
        else:
            sql = (
                f"INSERT INTO {table} ({column_names}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_target}) DO NOTHING"
            )
        self._connection.execute(sql, values)

    def opponent_exists(self, opponent_id: int, opponent_type: str) -> bool:
        table = {"team": "teams", "player": "players"}.get(opponent_type.casefold())
        if table is None:
            return False
        return (
            self._connection.execute(
                f"SELECT 1 FROM {table} WHERE id = ?", (opponent_id,)
            ).fetchone()
            is not None
        )

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()


@dataclass(frozen=True)
class DatabaseValidationResult:
    issues: tuple[str, ...]
    integrity: str
    foreign_key_violations: int

    @property
    def valid(self) -> bool:
        return not self.issues


def validate_database(
    path: Path, outcome_path: Path | None = None
) -> DatabaseValidationResult:
    """Validate a candidate database without modifying it."""
    if not path.is_file():
        return DatabaseValidationResult(
            issues=(f"Database does not exist: {path}",),
            integrity="missing",
            foreign_key_violations=0,
        )

    issues: list[str] = []
    integrity = "unknown"
    foreign_key_rows: list[tuple[Any, ...]] = []
    try:
        with closing(
            sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        ) as connection:
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity != "ok":
                issues.append(f"SQLite integrity_check failed: {integrity}")

            foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
            for table, rowid, parent, foreign_key in foreign_key_rows:
                issues.append(
                    f"Foreign-key violation: table={table}, rowid={rowid}, "
                    f"parent={parent}, fk={foreign_key}"
                )

            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if {"match_opponents", "teams", "players"}.issubset(tables):
                unsupported = connection.execute(
                    "SELECT id, match_id, opponent_id, opponent_type "
                    "FROM match_opponents "
                    "WHERE lower(opponent_type) NOT IN ('team', 'player')"
                ).fetchall()
                for row in unsupported:
                    issues.append(
                        f"Unsupported opponent type {row[3]!r} "
                        f"(match_opponents.id={row[0]}, match_id={row[1]}, "
                        f"opponent_id={row[2]})"
                    )

                unresolved = connection.execute(
                    "SELECT o.id, o.match_id, o.opponent_id, o.opponent_type "
                    "FROM match_opponents o "
                    "WHERE (lower(o.opponent_type) = 'team' AND NOT EXISTS "
                    "(SELECT 1 FROM teams t WHERE t.id = o.opponent_id)) "
                    "OR (lower(o.opponent_type) = 'player' AND NOT EXISTS "
                    "(SELECT 1 FROM players p WHERE p.id = o.opponent_id))"
                ).fetchall()
                for row in unresolved:
                    issues.append(
                        f"Unresolved {row[3]} opponent id={row[2]} "
                        f"(match_opponents.id={row[0]}, match_id={row[1]})"
                    )

                missing_opponents = connection.execute(
                    "SELECT COUNT(*) FROM matches m "
                    "WHERE m.status IN ('not_started', 'running') "
                    "AND NOT EXISTS "
                    "(SELECT 1 FROM match_opponents o WHERE o.match_id=m.id)"
                ).fetchone()[0]
                if missing_opponents:
                    log.warning(
                        "%d current/upcoming matches have no opponent rows; "
                        "reported as source-data incompleteness, not an FK failure",
                        missing_opponents,
                    )
    except sqlite3.Error as exc:
        issues.append(f"Cannot validate database {path}: {exc}")

    if outcome_path is not None:
        try:
            outcome = json.loads(outcome_path.read_text())
            unresolved_count = int(outcome.get("unresolved_relationships", -1))
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            issues.append(f"Cannot read valid scrape outcome {outcome_path}: {exc}")
        else:
            if unresolved_count != 0:
                issues.append(
                    f"Scrape reported {unresolved_count} unresolved relationship(s)"
                )

    return DatabaseValidationResult(
        issues=tuple(issues),
        integrity=integrity,
        foreign_key_violations=len(foreign_key_rows),
    )


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

    results_lookup: dict[int, int | None] = {
        result["team_id"]: result.get("score")
        for result in match_record.get("results", [])
        if "team_id" in result
    }

    rows: list[dict[str, Any]] = []
    for slot in match_record.get("opponents", []):
        opponent = slot.get("opponent") or {}
        opponent_id = opponent.get("id")
        if opponent_id is None:
            continue

        opponent_type = opponent.get("type") or slot.get("type") or "Unknown"
        normalized_type = opponent_type.casefold()
        if normalized_type not in {"team", "player"}:
            log.error(
                "Unsupported opponent type %r for match id=%s opponent id=%s",
                opponent_type,
                match_id,
                opponent_id,
            )
        rows.append(
            {
                "match_id": match_id,
                "opponent_id": opponent_id,
                "opponent_type": opponent_type,
                "score": results_lookup.get(opponent_id),
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
    to_row: Callable[[dict[str, Any]], dict[str, Any]],
    table: str,
    extra_rows_fn: Callable[[dict[str, Any]], list[dict[str, Any]]] | None = None,
    skip_fk_errors: bool = False,
    retry_records: tuple[dict[str, Any], ...] | None = None,
) -> ScrapeResult:
    """Fetch records or retry a deferred batch, committing each API page."""
    attempted = persisted = fk_rejected = 0
    missing_opponents = unsupported_opponents = 0
    records_to_retry: list[dict[str, Any]] = []
    api_incomplete = False

    def persist_record(record: dict[str, Any]) -> None:
        nonlocal attempted, persisted, fk_rejected
        nonlocal missing_opponents, unsupported_opponents
        attempted += 1
        row = to_row(record)
        try:
            db.upsert(table, row)
        except sqlite3.IntegrityError as exc:
            fk_rejected += 1
            log.error(
                "FK violation upserting into '%s' (record id=%s): %s; row=%s",
                table,
                record.get("id"),
                exc,
                row,
            )
            if not skip_fk_errors:
                raise
            records_to_retry.append(record)
            return

        persisted += 1
        if not extra_rows_fn:
            return

        opponents = record.get("opponents", [])
        if not opponents:
            missing_opponents += 1
            log.warning(
                "Match id=%s has no opponent entries in the API response",
                record.get("id"),
            )

        rejected = False
        for extra_row in extra_rows_fn(record):
            opponent_type = extra_row["opponent_type"]
            normalized_type = opponent_type.casefold()
            if normalized_type not in {"team", "player"}:
                unsupported_opponents += 1
                rejected = True
                log.error(
                    "Unsupported opponent type %r (match_id=%s, opponent_id=%s)",
                    opponent_type,
                    extra_row["match_id"],
                    extra_row["opponent_id"],
                )
                continue
            if not db.opponent_exists(extra_row["opponent_id"], opponent_type):
                fk_rejected += 1
                rejected = True
                log.error(
                    "Unresolved %s opponent id=%s for match_id=%s",
                    opponent_type,
                    extra_row["opponent_id"],
                    extra_row["match_id"],
                )
                continue
            try:
                db.upsert("match_opponents", extra_row)
            except sqlite3.IntegrityError as exc:
                fk_rejected += 1
                rejected = True
                log.error(
                    "FK violation upserting match_opponents "
                    "(match_id=%s, opponent_id=%s, opponent_type=%s): %s; row=%s",
                    extra_row["match_id"],
                    extra_row["opponent_id"],
                    opponent_type,
                    exc,
                    extra_row,
                )
                if not skip_fk_errors:
                    raise
        if rejected:
            records_to_retry.append(record)

    try:
        if retry_records is None:
            for page in client.fetch_all(endpoint):
                for record in page:
                    persist_record(record)
                db.commit()
        else:
            for record in retry_records:
                persist_record(record)
            db.commit()
    except (
        RateLimitError,
        ServerError,
        httpx.RequestError,
        httpx.HTTPStatusError,
    ) as exc:
        api_incomplete = True
        log.warning(
            "Request failed on /%s after %d persisted records; saving partial progress: %s",
            endpoint,
            persisted,
            exc,
        )

    result = ScrapeResult(
        attempted=attempted,
        persisted=persisted,
        fk_rejected=fk_rejected,
        api_incomplete=api_incomplete,
        unresolved_relationships=len(records_to_retry),
        missing_opponents=missing_opponents,
        unsupported_opponents=unsupported_opponents,
        records_to_retry=tuple(records_to_retry),
    )
    log.info(
        "/%s outcome: attempted=%d persisted=%d fk_rejected=%d unresolved=%d "
        "missing_opponents=%d unsupported_opponents=%d api_incomplete=%s",
        endpoint,
        result.attempted,
        result.persisted,
        result.fk_rejected,
        result.unresolved_relationships,
        result.missing_opponents,
        result.unsupported_opponents,
        result.api_incomplete,
    )
    return result


@dataclass(frozen=True)
class ResourceConfig:
    table: str
    to_row: Callable[[dict[str, Any]], dict[str, Any]]
    endpoint: str | None = None
    extra_rows_fn: Callable[[dict[str, Any]], list[dict[str, Any]]] | None = None
    skip_fk_errors: bool = False


RESOURCE_CONFIG: dict[str, ResourceConfig] = {
    "videogames": ResourceConfig(table="videogames", to_row=videogame_to_row),
    "leagues": ResourceConfig(table="leagues", to_row=league_to_row),
    "series": ResourceConfig(table="series", to_row=series_to_row),
    "series_upcoming": ResourceConfig(
        endpoint="series/upcoming",
        table="series",
        to_row=series_to_row,
        skip_fk_errors=True,
    ),
    "series_running": ResourceConfig(
        endpoint="series/running",
        table="series",
        to_row=series_to_row,
        skip_fk_errors=True,
    ),
    "tournaments": ResourceConfig(
        table="tournaments",
        to_row=tournament_to_row,
        skip_fk_errors=True,
    ),
    "tournaments_upcoming": ResourceConfig(
        endpoint="tournaments/upcoming",
        table="tournaments",
        to_row=tournament_to_row,
        skip_fk_errors=True,
    ),
    "tournaments_running": ResourceConfig(
        endpoint="tournaments/running",
        table="tournaments",
        to_row=tournament_to_row,
        skip_fk_errors=True,
    ),
    "matches": ResourceConfig(
        table="matches",
        to_row=match_to_row,
        extra_rows_fn=match_opponent_rows,
        skip_fk_errors=True,
    ),
    "matches_upcoming": ResourceConfig(
        endpoint="matches/upcoming",
        table="matches",
        to_row=match_to_row,
        extra_rows_fn=match_opponent_rows,
        skip_fk_errors=True,
    ),
    "matches_running": ResourceConfig(
        endpoint="matches/running",
        table="matches",
        to_row=match_to_row,
        extra_rows_fn=match_opponent_rows,
        skip_fk_errors=True,
    ),
    "teams": ResourceConfig(table="teams", to_row=team_to_row),
    "players": ResourceConfig(table="players", to_row=player_to_row),
}


def run_scrape(config: ScraperConfig, outcome_path: Path | None = None) -> int:
    """Scrape configured resources, retry deferred rows, and write an outcome."""
    unresolved_relationships = 0
    api_incomplete = False
    missing_opponents = 0
    unsupported_opponents = 0
    client = PandaScoreClient(
        api_key=config.api_key,
        page_size=config.page_size,
        since=config.since,
        page_delay=config.page_delay,
    )
    db = Database(path=config.db_path)
    try:
        db.ensure_schema()
        started_at = time.monotonic()
        deferred: list[tuple[str, ResourceConfig, ScrapeResult]] = []

        def scrape_one(resource: str, cfg: ResourceConfig) -> ScrapeResult:
            nonlocal api_incomplete, missing_opponents, unsupported_opponents
            result = scrape_resource(
                client,
                db,
                endpoint=cfg.endpoint or resource,
                table=cfg.table,
                to_row=cfg.to_row,
                extra_rows_fn=cfg.extra_rows_fn,
                skip_fk_errors=True,
            )
            api_incomplete |= result.api_incomplete
            missing_opponents += result.missing_opponents
            unsupported_opponents += result.unsupported_opponents
            return result

        for resource in config.resources:
            cfg = RESOURCE_CONFIG.get(resource)
            if cfg is None:
                log.warning("Unknown resource '%s' - skipping.", resource)
                continue
            result = scrape_one(resource, cfg)
            if result.records_to_retry:
                deferred.append((resource, cfg, result))

        for resource, cfg, first_result in deferred:
            refreshed: set[str] = set()

            def refresh_dependencies(parent: str) -> None:
                nonlocal unresolved_relationships
                if parent in refreshed:
                    return
                refreshed.add(parent)
                for ancestor in RESOURCE_DEPENDENCIES.get(parent, ()):
                    refresh_dependencies(ancestor)
                parent_cfg = RESOURCE_CONFIG.get(parent)
                if parent_cfg is not None:
                    parent_result = scrape_one(parent, parent_cfg)
                    unresolved_relationships += parent_result.unresolved_relationships

            for parent in RESOURCE_DEPENDENCIES.get(resource, ()):
                refresh_dependencies(parent)

            if cfg.extra_rows_fn is match_opponent_rows:
                opponent_parents = {
                    row["opponent_type"].casefold()
                    for record in first_result.records_to_retry
                    for row in match_opponent_rows(record)
                    if row["opponent_type"].casefold() in {"team", "player"}
                }
                for parent in opponent_parents:
                    parent_cfg = RESOURCE_CONFIG[parent + "s"]
                    parent_result = scrape_one(parent + "s", parent_cfg)
                    unresolved_relationships += parent_result.unresolved_relationships

            retry_result = scrape_resource(
                client,
                db,
                endpoint=cfg.endpoint or resource,
                table=cfg.table,
                to_row=cfg.to_row,
                extra_rows_fn=cfg.extra_rows_fn,
                skip_fk_errors=True,
                retry_records=first_result.records_to_retry,
            )
            api_incomplete |= retry_result.api_incomplete
            missing_opponents += retry_result.missing_opponents
            unsupported_opponents += retry_result.unsupported_opponents
            unresolved_relationships += retry_result.unresolved_relationships

        if outcome_path is not None:
            outcome_path.parent.mkdir(parents=True, exist_ok=True)
            outcome_path.write_text(
                json.dumps(
                    {
                        "unresolved_relationships": unresolved_relationships,
                        "api_incomplete": api_incomplete,
                        "missing_opponents": missing_opponents,
                        "unsupported_opponents": unsupported_opponents,
                    },
                    indent=2,
                )
                + "\n"
            )
        log.info("Scrape complete in %.1fs.", time.monotonic() - started_at)
    finally:
        client.close()
        db.close()

    return unresolved_relationships


def _parse_since(value: str) -> str:
    """Convert a human-friendly shorthand (e.g. ``48h``, ``7d``) to an
    ISO-8601 UTC datetime string, or return the value unchanged if it is
    already an ISO-8601 string."""
    m = re.fullmatch(r"(\d+)([hd])", value.strip())
    if m:
        amount, unit = int(m.group(1)), m.group(2)
        delta = timedelta(hours=amount) if unit == "h" else timedelta(days=amount)
        return (datetime.now(timezone.utc) - delta).strftime("%Y-%m-%dT%H:%M:%SZ")

    return value


def _build_config(
    api_key: str,
    db: Path,
    resources: list[str],
    page_size: int,
    since: str | None,
    page_delay: float,
) -> ScraperConfig:
    unknown = set(resources) - set(ALL_RESOURCES)
    if unknown:
        log.warning("Unrecognised resources will be skipped: %s", unknown)

    valid = [r for r in resources if r in ALL_RESOURCES]
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

    resolved_since: str | None = None
    if since:
        resolved_since = _parse_since(since)
        log.info(
            "Incremental mode: filtering matches to begin_at >= %s", resolved_since
        )

    return ScraperConfig(
        api_key=api_key,
        db_path=db,
        resources=tuple(valid),
        page_size=page_size,
        since=resolved_since,
        page_delay=page_delay,
    )


@app.command()
def main(
    db: Annotated[
        Path, typer.Option(help="SQLite database file path.")
    ] = DEFAULT_DB_PATH,
    resource: Annotated[
        list[str] | None,
        typer.Option(
            "--resource",
            help=f"Resource to scrape. Repeatable. Options: {', '.join(ALL_RESOURCES)}",
        ),
    ] = None,
    page_size: Annotated[
        int, typer.Option(help="Records per API page.")
    ] = DEFAULT_PAGE_SIZE,
    since: Annotated[
        str | None,
        typer.Option(
            help="Only fetch matches with begin_at >= WHEN. Accepts ISO-8601 or shorthand 48h/7d."
        ),
    ] = None,
    page_delay: Annotated[
        float, typer.Option(help="Seconds between paginated requests.")
    ] = INTER_PAGE_DELAY_SECONDS,
    outcome: Annotated[
        Path | None,
        typer.Option("--outcome", help="Write/read the scrape outcome JSON path."),
    ] = None,
    validate_db: Annotated[
        bool,
        typer.Option(
            "--validate-db", help="Validate the database and exit without scraping."
        ),
    ] = False,
    count: Annotated[
        bool,
        typer.Option(
            "--count/--no-count", help="Print remote totals without scraping."
        ),
    ] = False,
) -> None:
    """Scrape PandaScore resources or validate a database candidate."""
    if validate_db:
        result = validate_database(db, outcome)
        if result.valid:
            log.info(
                "Database validation passed (integrity=%s, no FK violations).",
                result.integrity,
            )
            return
        for issue in result.issues:
            log.error("Database validation failed: %s", issue)
        raise typer.Exit(code=1)

    api_key = os.environ.get("PANDASCORE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Error: PANDASCORE_API_KEY environment variable is not set.")

    resources = resource or list(ALL_RESOURCES)
    if count:
        with closing(PandaScoreClient(api_key=api_key)) as client:
            for res in resources:
                cfg = RESOURCE_CONFIG.get(res)
                if cfg is None:
                    log.info("%s: unknown resource", res)
                    continue
                endpoint = cfg.endpoint or res
                total = client.fetch_total(endpoint)
                pages = ((total - 1) // DEFAULT_PAGE_SIZE + 1) if total else "?"
                delay_min = (
                    (pages if isinstance(pages, int) else 0) * INTER_PAGE_DELAY_SECONDS
                ) / 60
                log.info(
                    "%s: %d records ~%d pages ~%.1f min delay",
                    endpoint,
                    total,
                    pages,
                    delay_min,
                )
        return

    unresolved = run_scrape(
        _build_config(api_key, db, resources, page_size, since, page_delay), outcome
    )
    if unresolved:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
