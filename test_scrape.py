#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "hishel>=1.2",
#   "httpx>=0.27",
#   "tenacity>=9.0",
#   "typer>=0.12",
#   "pytest>=8.0",
# ]
# ///
import json
import sqlite3
from pathlib import Path
from statistics import median
from time import perf_counter
from unittest.mock import MagicMock, call, patch

import pytest

import scrape


def _metadata_fixture_connection():
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys=ON")
    for statement in scrape.SCHEMA_DDL:
        connection.execute(statement)

    connection.executemany(
        "INSERT INTO videogames(id, name, slug) VALUES (?, ?, ?)",
        [(1, "Test Game", "test-game"), (2, "Other Game", "other-game")],
    )
    connection.executemany(
        "INSERT INTO leagues(id, name, slug, videogame_id) VALUES (?, ?, ?, ?)",
        [(1, "Test League", "test-league", 1), (2, "Other League", "other-league", 2)],
    )
    connection.executemany(
        "INSERT INTO series(id, name, full_name, league_id, videogame_id) VALUES (?, ?, ?, ?, ?)",
        [
            (1, "Test Series", "Test Series Full Name", 1, 1),
            (2, "Other Series", None, 2, 2),
        ],
    )
    connection.executemany(
        "INSERT INTO tournaments(id, name, begin_at, end_at, serie_id, league_id, "
        "videogame_id, tier, winner_id, winner_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "Test Cup", "2020-01-01", "2020-01-02", 1, 1, 1, "s", 1, "Team"),
            (2, "Other Cup", "2020-01-01", "2020-01-02", 2, 2, 2, "a", 2, "Team"),
            (3, "No Series Completed", "2020-02-01", "2020-02-02", None, 1, 1, "s", None, None),
            (4, "No Series Ongoing", None, None, None, 1, 1, "b", None, None),
            (5, "No Series Upcoming", "2099-01-01", "2099-01-02", None, 1, 1, "c", None, None),
        ],
    )
    connection.execute(
        "UPDATE tournaments SET begin_at = datetime('now', '-1 day'), "
        "end_at = datetime('now', '+1 day') WHERE id = 4"
    )
    connection.executemany(
        "INSERT INTO teams(id, name, slug, acronym, current_videogame_id) VALUES (?, ?, ?, ?, ?)",
        [
            (1, "Test Team", "test-team", "TST", 1),
            (2, "Rival Team", "rival-team", "RIV", 1),
            (3, "Other Team", "other-team", "OTH", 2),
        ],
    )
    connection.executemany(
        "INSERT INTO players(id, name, active, current_team_id, current_videogame_id) "
        "VALUES (?, ?, ?, ?, ?)",
        [(1, "Active Player", 1, 1, 1), (2, "Inactive Player", 0, 1, 1),
         (3, "Other Player", 1, 3, 2)],
    )
    connection.executemany(
        "INSERT INTO matches(id, name, tournament_id, serie_id, league_id, videogame_id, "
        "status, match_type, number_of_games, scheduled_at, begin_at, end_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'best_of', 3, ?, ?, ?)",
        [
            (1, "Test Live", 1, 1, 1, 1, "running", "2025-01-01", "2025-01-01", None),
            (2, "Test Upcoming", 1, 1, 1, 1, "not_started", "2099-01-01", None, None),
            (3, "Test Finished", 1, 1, 1, 1, "finished", "2020-01-02", "2020-01-02", "2020-01-02"),
            (4, "Other Finished", 2, 2, 2, 2, "finished", "2020-01-02", "2020-01-02", "2020-01-02"),
            (5, "No Series Finished", 3, None, 1, 1, "finished", "2020-02-02", "2020-02-02", "2020-02-02"),
        ],
    )
    connection.execute(
        "UPDATE matches SET streams_raw_url = ? WHERE id = ?",
        ("https://www.twitch.tv/test-stream", 2),
    )
    connection.executemany(
        "INSERT INTO match_opponents(match_id, opponent_id, opponent_type, score, is_winner) "
        "VALUES (?, ?, 'Team', ?, ?)",
        [(1, 1, 1, 1), (1, 2, 0, 0), (2, 1, None, None), (2, 2, None, None),
         (3, 1, 2, 0), (3, 2, 3, 1), (4, 2, 2, 1), (4, 3, 1, 0)],
    )
    return connection


def make_client(since=None, page_size=3, page_delay=0.0):
    """Return a PandaScoreClient with all HTTP internals mocked out."""
    with patch("scrape.SyncSqliteStorage"), patch("scrape.SyncCacheClient"):
        return scrape.PandaScoreClient(
            api_key="test-key",
            page_size=page_size,
            since=since,
            page_delay=page_delay,
        )


def test_fetch_all_since_stops_early_when_page_crosses_cutoff():
    """
    A page that mixes records above and below the cutoff should:
      - yield only the records >= since
      - NOT fetch any further pages (early-termination)

    This pins the fragile `len(filtered) < len(records)` break logic.
    """
    client = make_client(since="2025-01-10T00:00:00Z")

    page1 = [
        {"id": 1, "begin_at": "2025-01-12T00:00:00Z"},  # above cutoff
        {"id": 2, "begin_at": "2025-01-11T00:00:00Z"},  # above cutoff
        {"id": 3, "begin_at": "2025-01-08T00:00:00Z"},  # BELOW cutoff
    ]

    with patch.object(
        client,
        "_fetch_page_with_retry",
        return_value=scrape.PageResult(records=page1, from_cache=False),
    ) as mock_fetch:
        results = list(client.fetch_all("matches"))

    # Only the two records above the cutoff should be yielded
    assert results == [
        [
            {"id": 1, "begin_at": "2025-01-12T00:00:00Z"},
            {"id": 2, "begin_at": "2025-01-11T00:00:00Z"},
        ]
    ]
    # Page 2 must never be requested - the loop should have broken after page 1
    mock_fetch.assert_called_once_with("matches", 1)


def test_fetch_all_since_yields_nothing_when_all_records_before_cutoff():
    """
    When every record on the first page predates the cutoff, nothing should be
    yielded and no further pages should be fetched.
    """
    client = make_client(since="2025-01-10T00:00:00Z")

    page1 = [
        {"id": 1, "begin_at": "2025-01-08T00:00:00Z"},  # below cutoff
        {"id": 2, "begin_at": "2025-01-07T00:00:00Z"},  # below cutoff
    ]

    with patch.object(
        client,
        "_fetch_page_with_retry",
        return_value=scrape.PageResult(records=page1, from_cache=False),
    ) as mock_fetch:
        results = list(client.fetch_all("matches"))

    assert results == []
    mock_fetch.assert_called_once_with("matches", 1)


def test_fetch_all_since_silently_drops_records_missing_begin_at():
    """
    Records with begin_at=None (or the key absent entirely) are silently
    excluded from the yielded page. This confirms - and documents - the
    silent-drop behaviour so a future refactor doesn't change it unknowingly.
    """
    client = make_client(since="2025-01-10T00:00:00Z")

    page1 = [
        {"id": 1, "begin_at": "2025-01-12T00:00:00Z"},  # valid - included
        {"id": 2, "begin_at": None},  # None - silently dropped
        {"id": 3},  # key absent - silently dropped
    ]

    with patch.object(
        client,
        "_fetch_page_with_retry",
        return_value=scrape.PageResult(records=page1, from_cache=False),
    ):
        results = list(client.fetch_all("matches"))

    assert results == [[{"id": 1, "begin_at": "2025-01-12T00:00:00Z"}]]


def test_fetch_all_no_since_yields_all_pages_and_stops_on_partial():
    """
    Without --since, fetch_all must yield every page and stop naturally when a
    page has fewer records than page_size (the standard pagination sentinel).
    """
    client = make_client(page_size=3)  # no since

    full_page = [{"id": i} for i in range(1, 4)]  # 3 records == page_size
    partial_page = [{"id": 4}]  # 1 record < page_size → last page

    with patch.object(
        client,
        "_fetch_page_with_retry",
        side_effect=[
            scrape.PageResult(records=full_page, from_cache=False),
            scrape.PageResult(records=partial_page, from_cache=False),
        ],
    ) as mock_fetch:
        results = list(client.fetch_all("leagues"))

    assert results == [full_page, partial_page]
    assert mock_fetch.call_count == 2


def test_match_opponent_rows_preserves_api_type_casing():
    """
    FAILING before fix.

    The scraper should preserve the raw PandaScore payload in the DB. PandaScore
    returns "type": "Team" (PascalCase), so match_opponent_rows() must store
    that value unchanged. Query normalization belongs in metadata.json.
    """
    record = {
        "id": 1,
        "winner_id": None,
        "opponents": [
            {"type": "Team", "opponent": {"id": 10}},
        ],
        "results": [],
    }
    rows = scrape.match_opponent_rows(record)
    assert rows[0]["opponent_type"] == "Team"


def test_match_opponent_rows_score_from_results_array():
    """
    FAILING before fix.

    The PandaScore API does NOT include a score field inside opponent slots.
    Scores are in a separate top-level 'results' array:
        [{'score': N, 'team_id': T}, ...]
    The function must look up each opponent's score from that array.
    """
    record = {
        "id": 42,
        "winner_id": 10,
        "opponents": [
            {"type": "Team", "opponent": {"id": 10}},
            {"type": "Team", "opponent": {"id": 20}},
        ],
        "results": [
            {"score": 2, "team_id": 10},
            {"score": 0, "team_id": 20},
        ],
    }
    rows = scrape.match_opponent_rows(record)
    scores = {r["opponent_id"]: r["score"] for r in rows}
    assert scores == {10: 2, 20: 0}


def test_match_opponent_type_case_variants_are_preserved():
    for raw_type in ("Team", "team", "Player", "player"):
        record = {
            "id": 100,
            "opponents": [{"type": raw_type, "opponent": {"id": 55}}],
            "results": [],
        }
        rows = scrape.match_opponent_rows(record)
        assert rows[0]["opponent_type"] == raw_type


def test_match_opponent_unknown_type_is_preserved_for_validation():
    record = {
        "id": 101,
        "opponents": [{"type": "Coach", "opponent": {"id": 56}}],
        "results": [],
    }
    rows = scrape.match_opponent_rows(record)
    assert rows[0]["opponent_type"] == "Coach"


def test_match_opponent_type_case_variants_are_preserved():
    for raw_type in ("Team", "team", "Player", "player"):
        rows = scrape.match_opponent_rows(
            {
                "id": 100,
                "opponents": [{"type": raw_type, "opponent": {"id": 55}}],
                "results": [],
            }
        )
        assert rows[0]["opponent_type"] == raw_type


def test_match_opponent_unknown_type_is_preserved_for_validation():
    rows = scrape.match_opponent_rows(
        {
            "id": 101,
            "opponents": [{"type": "Coach", "opponent": {"id": 56}}],
            "results": [],
        }
    )
    assert rows[0]["opponent_type"] == "Coach"


def test_match_opponent_rows_basic():
    """
    Two opponents with a declared winner: the winning opponent gets is_winner=1,
    the loser gets is_winner=0.  Uses real API shape: slot-level 'type',
    scores from the top-level 'results' array, no score field in slots.
    """
    record = {
        "id": 42,
        "winner_id": 10,
        "opponents": [
            {"type": "Team", "opponent": {"id": 10}},
            {"type": "Team", "opponent": {"id": 20}},
        ],
        "results": [
            {"score": 2, "team_id": 10},
            {"score": 0, "team_id": 20},
        ],
    }

    rows = scrape.match_opponent_rows(record)

    assert rows == [
        {
            "match_id": 42,
            "opponent_id": 10,
            "opponent_type": "Team",
            "score": 2,
            "is_winner": 1,
        },
        {
            "match_id": 42,
            "opponent_id": 20,
            "opponent_type": "Team",
            "score": 0,
            "is_winner": 0,
        },
    ]


def test_match_opponent_rows_slot_without_opponent_is_skipped():
    """
    A slot where the 'opponent' key is missing (or None) must be skipped
    entirely - no row should be produced for it.
    """
    record = {
        "id": 99,
        "winner_id": None,
        "opponents": [
            {"type": "Team", "opponent": {"id": 5}},
            {"type": "Team"},  # no 'opponent' key
            {"type": "Team", "opponent": None},  # opponent is None
        ],
        "results": [],
    }

    rows = scrape.match_opponent_rows(record)

    assert len(rows) == 1
    assert rows[0]["opponent_id"] == 5


def test_match_opponent_rows_no_winner_sets_is_winner_none():
    """
    When winner_id is None (match not yet finished), is_winner must be None
    for every row - not 0 - because 0 implies a known loser.
    Score is also None because upcoming matches have no results yet.
    """
    record = {
        "id": 7,
        "winner_id": None,
        "opponents": [
            {"type": "Team", "opponent": {"id": 1}},
            {"type": "Team", "opponent": {"id": 2}},
        ],
        "results": [],
    }

    rows = scrape.match_opponent_rows(record)

    assert all(row["is_winner"] is None for row in rows)


def test_parse_since_hours():
    """'48h' should produce an ISO-8601 UTC string ~48 hours in the past."""
    from datetime import datetime, timedelta, timezone

    result = scrape._parse_since("48h")
    parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    expected = datetime.now(timezone.utc) - timedelta(hours=48)

    # _parse_since truncates to whole seconds; allow ±2s tolerance
    assert abs((parsed - expected).total_seconds()) < 2


def test_parse_since_days():
    """'7d' should produce an ISO-8601 UTC string ~7 days in the past."""
    from datetime import datetime, timedelta, timezone

    result = scrape._parse_since("7d")
    parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    expected = datetime.now(timezone.utc) - timedelta(days=7)

    assert abs((parsed - expected).total_seconds()) < 2


def test_parse_since_passthrough_iso8601():
    """An already-valid ISO-8601 string must be returned unchanged."""
    iso = "2025-03-15T12:00:00Z"
    assert scrape._parse_since(iso) == iso


def test_scrape_resource_reports_skipped_rows_as_unpersisted():
    """FK-rejected rows are attempted but do not count as persisted."""
    import sqlite3

    client = make_client()
    mock_db = MagicMock()
    records = [{"id": 1}, {"id": 2}, {"id": 3}]
    mock_db.upsert.side_effect = [
        sqlite3.IntegrityError("FK constraint failed"),
        None,
        None,
    ]

    with patch.object(client, "fetch_all", return_value=iter([records])):
        result = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="teams",
            to_row=lambda record: record,
            table="teams",
            skip_fk_errors=True,
        )

    assert result.attempted == 3
    assert result.persisted == 2
    assert result.fk_rejected == 1
    assert result.unresolved_relationships == 1
    assert [record["id"] for record in result.records_to_retry] == [1]
    mock_db.commit.assert_called_once()


def test_scrape_resource_rejects_unresolved_opponent_reference():
    client = make_client()
    mock_db = MagicMock()
    mock_db.opponent_exists.return_value = False
    records = [
        {
            "id": 7,
            "opponents": [{"type": "Team", "opponent": {"id": 999}}],
        }
    ]

    with patch.object(client, "fetch_all", return_value=iter([records])):
        result = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="matches/upcoming",
            to_row=lambda record: {"id": record["id"]},
            table="matches",
            extra_rows_fn=scrape.match_opponent_rows,
            skip_fk_errors=True,
        )

    assert result.fk_rejected == 1
    assert result.unresolved_relationships == 1
    assert mock_db.upsert.call_args_list == [call("matches", {"id": 7})]


def test_database_match_opponent_retry_does_not_replace_existing_row(tmp_path):
    path = tmp_path / "upsert.db"
    db = scrape.Database(path)
    db.ensure_schema()
    db.upsert("videogames", {"id": 1, "name": "game", "slug": "game"})
    db.upsert(
        "leagues", {"id": 2, "name": "league", "slug": "league", "videogame_id": 1}
    )
    db.upsert("series", {"id": 3, "name": "series", "league_id": 2, "videogame_id": 1})
    db.upsert(
        "tournaments",
        {"id": 4, "name": "event", "serie_id": 3, "league_id": 2, "videogame_id": 1},
    )
    db.upsert(
        "matches",
        {"id": 5, "tournament_id": 4, "serie_id": 3, "league_id": 2, "videogame_id": 1},
    )
    db.upsert("teams", {"id": 6, "name": "team", "slug": "team"})
    row = {
        "match_id": 5,
        "opponent_id": 6,
        "opponent_type": "Team",
        "score": None,
        "is_winner": None,
    }
    db.upsert("match_opponents", row)
    db.upsert("match_opponents", {**row, "score": 2})
    result = db._connection.execute(
        "SELECT score FROM match_opponents WHERE match_id=5 AND opponent_id=6"
    ).fetchone()
    db.close()

    assert result == (2,)


def test_ensure_schema_migrates_legacy_columns_and_is_repeatable(tmp_path):
    db = scrape.Database(tmp_path / "legacy.db")
    db.ensure_schema()
    db._connection.execute(
        "ALTER TABLE matches RENAME COLUMN streams_raw_url TO live_embed_url"
    )
    db._connection.execute("ALTER TABLE matches ADD COLUMN complete INTEGER")
    db._connection.execute("ALTER TABLE tournaments ADD COLUMN full_name TEXT")
    db.commit()

    db.ensure_schema()
    db.ensure_schema()

    columns = {
        table: {
            row[1] for row in db._connection.execute(f"PRAGMA table_info({table})")
        }
        for table in ("matches", "tournaments")
    }
    db.close()

    assert "complete" not in columns["matches"]
    assert "full_name" not in columns["tournaments"]
    assert "live_embed_url" not in columns["matches"]
    assert "streams_raw_url" in columns["matches"]


def test_ensure_schema_preserves_rows_and_relationships_when_migrating(tmp_path):
    db = scrape.Database(tmp_path / "populated-legacy.db")
    db.ensure_schema()
    db.upsert("videogames", {"id": 1, "name": "game", "slug": "game"})
    db.upsert(
        "leagues", {"id": 2, "name": "league", "slug": "league", "videogame_id": 1}
    )
    db.upsert("series", {"id": 3, "name": "series", "league_id": 2, "videogame_id": 1})
    db.upsert(
        "tournaments",
        {
            "id": 4,
            "name": "event",
            "serie_id": 3,
            "league_id": 2,
            "videogame_id": 1,
        },
    )
    db.upsert(
        "matches",
        {
            "id": 5,
            "tournament_id": 4,
            "serie_id": 3,
            "league_id": 2,
            "videogame_id": 1,
            "streams_raw_url": "https://www.twitch.tv/legacy",
        },
    )
    db.upsert("teams", {"id": 6, "name": "team", "slug": "team"})
    db.upsert(
        "match_opponents",
        {
            "match_id": 5,
            "opponent_id": 6,
            "opponent_type": "Team",
            "score": 2,
            "is_winner": 1,
        },
    )
    db.commit()
    db._connection.execute(
        "ALTER TABLE matches RENAME COLUMN streams_raw_url TO live_embed_url"
    )
    db._connection.execute("ALTER TABLE matches ADD COLUMN complete INTEGER")
    db._connection.execute("ALTER TABLE tournaments ADD COLUMN full_name TEXT")
    db._connection.execute("UPDATE matches SET complete = 1 WHERE id = 5")
    db._connection.execute("UPDATE tournaments SET full_name = 'obsolete value' WHERE id = 4")
    db.commit()

    db.ensure_schema()

    counts = {
        table: db._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("videogames", "leagues", "series", "tournaments", "matches", "teams", "match_opponents")
    }
    foreign_key_violations = db._connection.execute("PRAGMA foreign_key_check").fetchall()
    migrated_url = db._connection.execute(
        "SELECT streams_raw_url FROM matches WHERE id = 5"
    ).fetchone()[0]
    db.close()

    assert counts == {
        "videogames": 1,
        "leagues": 1,
        "series": 1,
        "tournaments": 1,
        "matches": 1,
        "teams": 1,
        "match_opponents": 1,
    }
    assert foreign_key_violations == []
    assert migrated_url == "https://www.twitch.tv/legacy"


def test_ensure_schema_rejects_conflicting_stream_url_columns_without_data_loss(tmp_path):
    db = scrape.Database(tmp_path / "conflicting.db")
    db.ensure_schema()
    db._connection.execute("ALTER TABLE matches ADD COLUMN live_embed_url TEXT")
    db._connection.execute(
        "INSERT INTO matches(id, streams_raw_url, live_embed_url) "
        "VALUES (1, 'new-value', 'old-value')"
    )
    db.commit()

    with pytest.raises(sqlite3.DatabaseError, match="both live_embed_url and streams_raw_url"):
        db.ensure_schema()

    values = db._connection.execute(
        "SELECT streams_raw_url, live_embed_url FROM matches WHERE id = 1"
    ).fetchone()
    db.close()
    assert values == ("new-value", "old-value")


def test_scrape_resource_reports_missing_opponents_without_fk_rejection():
    client = make_client()
    mock_db = MagicMock()
    records = [{"id": 7, "opponents": []}]

    with patch.object(client, "fetch_all", return_value=iter([records])):
        result = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="matches/upcoming",
            to_row=lambda record: {"id": record["id"]},
            table="matches",
            extra_rows_fn=scrape.match_opponent_rows,
            skip_fk_errors=True,
        )

    assert result.persisted == 1
    assert result.fk_rejected == 0
    assert result.missing_opponents == 1
    assert result.unresolved_relationships == 0


def test_scrape_resource_raises_on_fk_error_when_skip_is_false():
    """Without skip_fk_errors, an IntegrityError must propagate to the caller."""
    import sqlite3

    client = make_client()
    mock_db = MagicMock()
    mock_db.upsert.side_effect = sqlite3.IntegrityError("FK constraint failed")

    with patch.object(client, "fetch_all", return_value=iter([[{"id": 1}]])):
        import pytest

        with pytest.raises(sqlite3.IntegrityError):
            scrape.scrape_resource(
                client=client,
                db=mock_db,
                endpoint="teams",
                to_row=lambda r: r,
                table="teams",
                skip_fk_errors=False,
            )


def test_nested_id_extracts_id_from_dict():
    assert scrape._nested_id({"videogame": {"id": 7, "name": "CS2"}}, "videogame") == 7


def test_nested_id_returns_none_when_key_absent():
    assert scrape._nested_id({}, "videogame") is None


def test_nested_id_returns_none_when_value_is_not_dict():
    # API occasionally returns a scalar or null for nested objects
    assert scrape._nested_id({"videogame": None}, "videogame") is None
    assert scrape._nested_id({"videogame": 42}, "videogame") is None


def test_success_only_filter_allows_200():
    """HTTP 200 responses must be admitted to the cache."""
    f = scrape._SuccessOnlyFilter()
    assert f.apply(scrape.HishelResponse(status_code=200), None) is True


def test_success_only_filter_blocks_non_200():
    """4xx and 5xx responses must never be stored in the cache.

    Before the fix, FilterPolicy() cached all responses unconditionally. A
    transient 500 on page N would be stored and replayed on every retry,
    making tenacity exhaust its attempts against a cached error response
    rather than the real API.
    """
    f = scrape._SuccessOnlyFilter()
    assert f.apply(scrape.HishelResponse(status_code=429), None) is False
    assert f.apply(scrape.HishelResponse(status_code=500), None) is False
    assert f.apply(scrape.HishelResponse(status_code=503), None) is False


def test_fetch_all_stops_immediately_on_empty_first_page():
    """An empty first response must yield nothing and make only one request."""
    client = make_client()

    with patch.object(
        client,
        "_fetch_page_with_retry",
        return_value=scrape.PageResult(records=[], from_cache=False),
    ) as mock_fetch:
        results = list(client.fetch_all("videogames"))

    assert results == []
    mock_fetch.assert_called_once_with("videogames", 1)


def test_run_scrape_writes_partial_api_outcome_without_fk_failures(tmp_path):
    config = scrape.ScraperConfig(
        api_key="test",
        db_path=tmp_path / "candidate.db",
        resources=("videogames",),
        page_size=100,
    )
    client = make_client()
    with (
        patch("scrape.PandaScoreClient", return_value=client),
        patch("scrape.Database") as db_factory,
        patch("scrape.closing", side_effect=lambda value: value),
        patch.object(
            scrape,
            "scrape_resource",
            return_value=scrape.ScrapeResult(api_incomplete=True),
        ),
    ):
        db_factory.return_value.__enter__.return_value = MagicMock()
        outcome_path = tmp_path / "outcome.json"
        unresolved = scrape.run_scrape(config, outcome_path)

    assert unresolved == 0
    outcome = json.loads(outcome_path.read_text())
    assert outcome["api_incomplete"] is True
    assert outcome["unresolved_relationships"] == 0


def test_validate_database_rejects_foreign_key_violations(tmp_path):
    path = tmp_path / "invalid.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id REFERENCES parent(id));"
        "INSERT INTO child(id, parent_id) VALUES (1, 99);"
    )
    connection.close()

    result = scrape.validate_database(path)

    assert not result.valid
    assert result.foreign_key_violations == 1


def test_validate_database_allows_valid_partial_api_refresh(tmp_path):
    path = tmp_path / "valid.db"
    outcome = tmp_path / "outcome.json"
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id REFERENCES parent(id));"
        "INSERT INTO parent(id) VALUES (1);"
        "INSERT INTO child(id, parent_id) VALUES (2, 1);"
    )
    connection.close()
    outcome.write_text('{"unresolved_relationships": 0, "api_incomplete": true}')

    result = scrape.validate_database(path, outcome)

    assert result.valid
    assert result.integrity == "ok"
    assert result.foreign_key_violations == 0


def test_validate_database_rejects_unresolved_outcome(tmp_path):
    path = tmp_path / "valid.db"
    outcome = tmp_path / "outcome.json"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE item(id INTEGER PRIMARY KEY)")
    connection.close()
    outcome.write_text('{"unresolved_relationships": 2}')

    result = scrape.validate_database(path, outcome)

    assert not result.valid
    assert any("unresolved relationship" in issue for issue in result.issues)


def test_validate_database_accepts_case_variants_and_rejects_unknown_type(tmp_path):
    path = tmp_path / "opponents.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE matches(id INTEGER PRIMARY KEY, status TEXT);"
        "CREATE TABLE teams(id INTEGER PRIMARY KEY);"
        "CREATE TABLE players(id INTEGER PRIMARY KEY);"
        "CREATE TABLE match_opponents(id INTEGER PRIMARY KEY, match_id INTEGER, "
        "opponent_id INTEGER, opponent_type TEXT);"
        "INSERT INTO matches VALUES (1, 'running');"
        "INSERT INTO teams VALUES (10);"
        "INSERT INTO players VALUES (20);"
        "INSERT INTO match_opponents VALUES (1, 1, 10, 'team');"
        "INSERT INTO match_opponents VALUES (2, 1, 20, 'Player');"
    )
    connection.close()

    result = scrape.validate_database(path)
    assert result.valid

    connection = sqlite3.connect(path)
    connection.execute("INSERT INTO match_opponents VALUES (3, 1, 30, 'Coach')")
    connection.commit()
    connection.close()

    result = scrape.validate_database(path)
    assert not result.valid
    assert any("Unsupported opponent type 'Coach'" in issue for issue in result.issues)


def test_validate_database_rejects_unresolved_scrape_outcome(tmp_path):
    db_path = tmp_path / "candidate.db"
    outcome_path = tmp_path / "outcome.json"
    connection = __import__("sqlite3").connect(db_path)
    connection.execute("CREATE TABLE item(id INTEGER PRIMARY KEY)")
    connection.commit()
    connection.close()
    outcome_path.write_text('{"unresolved_relationships": 2}')

    result = scrape.validate_database(db_path, outcome_path)

    assert result.valid is False
    assert any("unresolved relationship" in issue for issue in result.issues)


def test_fetch_all_live_request_sleeps_between_full_pages():
    """time.sleep must be called once between two live full pages."""
    client = make_client(page_size=2, page_delay=1.5)

    full_page = [{"id": 1}, {"id": 2}]
    partial_page = [{"id": 3}]

    with patch.object(
        client,
        "_fetch_page_with_retry",
        side_effect=[
            scrape.PageResult(records=full_page, from_cache=False),
            scrape.PageResult(records=partial_page, from_cache=False),
        ],
    ):
        with patch("scrape.time.sleep") as mock_sleep:
            list(client.fetch_all("leagues"))

    mock_sleep.assert_called_once_with(1.5)


def test_fetch_all_cache_hit_does_not_sleep():
    """Cache hits must not count against the rate-limit delay budget."""
    client = make_client(page_size=2, page_delay=1.5)

    full_page = [{"id": 1}, {"id": 2}]
    partial_page = [{"id": 3}]

    with patch.object(
        client,
        "_fetch_page_with_retry",
        side_effect=[
            scrape.PageResult(records=full_page, from_cache=True),  # cache hit
            scrape.PageResult(records=partial_page, from_cache=False),
        ],
    ):
        with patch("scrape.time.sleep") as mock_sleep:
            list(client.fetch_all("leagues"))

    mock_sleep.assert_not_called()


def test_fetch_all_since_continues_when_entire_page_is_above_cutoff():
    """
    When all records on a full page are above the cutoff, fetch_all must
    continue to the next page rather than stopping early.

    This exercises the `len(filtered) == len(records)` path - the break only
    fires when some records fall below the cutoff.
    """
    client = make_client(since="2025-01-01T00:00:00Z", page_size=2)

    page1 = [
        {"id": 1, "begin_at": "2025-01-15T00:00:00Z"},
        {"id": 2, "begin_at": "2025-01-14T00:00:00Z"},
    ]
    page2 = [{"id": 3, "begin_at": "2025-01-13T00:00:00Z"}]  # partial → last page

    with patch.object(
        client,
        "_fetch_page_with_retry",
        side_effect=[
            scrape.PageResult(records=page1, from_cache=False),
            scrape.PageResult(records=page2, from_cache=False),
        ],
    ) as mock_fetch:
        results = list(client.fetch_all("matches"))

    assert results == [page1, page2]
    assert mock_fetch.call_count == 2


def test_scrape_resource_calls_extra_rows_fn_and_upserts_junction_rows():
    """
    When extra_rows_fn is provided, junction rows must be upserted into
    'match_opponents' in addition to the main table row.
    """
    client = make_client()
    mock_db = MagicMock()

    records = [
        {
            "id": 1,
            "name": "Match A",
            "opponents": [{"type": "Team", "opponent": {"id": 10}}],
            "results": [{"score": 2, "team_id": 10}],
            "winner_id": 10,
        }
    ]

    with patch.object(client, "fetch_all", return_value=iter([records])):
        scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="matches",
            to_row=scrape.match_to_row,
            table="matches",
            extra_rows_fn=scrape.match_opponent_rows,
        )

    tables_upserted = [call[0][0] for call in mock_db.upsert.call_args_list]
    assert tables_upserted == ["matches", "match_opponents"]


def test_run_scrape_retries_fk_rejected_record_after_refreshing_parents():
    player = {
        "id": 4,
        "name": "player",
        "current_team": {"id": 9},
        "current_videogame": {"id": 1},
    }
    config = scrape.ScraperConfig(
        api_key="test",
        db_path=Path("unused.db"),
        resources=("players",),
        page_size=100,
    )
    client = make_client()
    db = MagicMock()
    first_result = scrape.ScrapeResult(
        attempted=1,
        fk_rejected=1,
        unresolved_relationships=1,
        records_to_retry=(player,),
    )

    with (
        patch("scrape.PandaScoreClient", return_value=client),
        patch("scrape.Database", return_value=db),
        patch.object(
            scrape,
            "scrape_resource",
            side_effect=[
                first_result,
                scrape.ScrapeResult(attempted=1, persisted=1),
                scrape.ScrapeResult(attempted=1, persisted=1),
                scrape.ScrapeResult(attempted=1, persisted=1),
            ],
        ) as mock_scrape,
    ):
        unresolved = scrape.run_scrape(config)

    assert unresolved == 0
    assert mock_scrape.call_count == 4
    assert [call.kwargs.get("endpoint") for call in mock_scrape.call_args_list] == [
        "players",
        "videogames",
        "teams",
        "players",
    ]
    assert mock_scrape.call_args_list[-1].kwargs["retry_records"] == (player,)


def test_scrape_resource_saves_partial_progress_on_server_error():
    """
    When fetch_all raises ServerError mid-iteration, scrape_resource must
    commit what was already processed and return the partial count.
    """
    client = make_client()
    mock_db = MagicMock()
    page1 = [{"id": 1}, {"id": 2}]

    def _raises_after_first_page(endpoint):
        yield page1
        raise scrape.ServerError("500 on /matches page 2")

    with patch.object(client, "fetch_all", side_effect=_raises_after_first_page):
        result = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="matches",
            to_row=lambda r: r,
            table="matches",
        )

    assert result.persisted == 2
    assert result.api_incomplete is True
    mock_db.commit.assert_called_once()


def test_scrape_resource_saves_partial_progress_on_rate_limit_error():
    """
    When fetch_all raises RateLimitError mid-iteration, scrape_resource must
    commit what was already processed and return the partial count.
    """
    client = make_client()
    mock_db = MagicMock()
    page1 = [{"id": 1}]

    def _raises_after_first_page(endpoint):
        yield page1
        raise scrape.RateLimitError("429 on /matches page 2")

    with patch.object(client, "fetch_all", side_effect=_raises_after_first_page):
        result = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="matches",
            to_row=lambda r: r,
            table="matches",
        )

    assert result.persisted == 1
    assert result.api_incomplete is True
    mock_db.commit.assert_called_once()


def test_match_opponent_rows_empty_opponents_list():
    """An empty opponents list must produce an empty result."""
    record = {"id": 1, "winner_id": None, "opponents": [], "results": []}
    assert scrape.match_opponent_rows(record) == []


def test_match_opponent_rows_score_is_none_when_no_results():
    """
    Documents expected API behaviour: upcoming/running matches have no
    'results' entry yet, so score must be None (not a bug).
    """
    record = {
        "id": 9,
        "winner_id": None,
        "opponents": [{"type": "Team", "opponent": {"id": 5}}],
        "results": [],
    }
    rows = scrape.match_opponent_rows(record)
    assert rows[0]["score"] is None


def test_match_opponent_rows_score_is_none_when_team_absent_from_results():
    """
    If a team_id appears in opponents but not in results (partial data),
    the score must safely default to None.
    """
    record = {
        "id": 10,
        "winner_id": None,
        "opponents": [{"type": "Team", "opponent": {"id": 99}}],
        "results": [{"score": 3, "team_id": 77}],  # different team
    }
    rows = scrape.match_opponent_rows(record)
    assert rows[0]["score"] is None


def test_tournament_to_row_omits_unsupported_full_name():
    record = {
        "id": 1,
        "name": "Group A",
        "full_name": None,
        "slug": "group-a",
        "begin_at": None,
        "end_at": None,
        "serie": None,
        "league": None,
        "videogame": None,
        "tier": "a",
        "has_bracket": False,
        "live_supported": False,
        "detailed_stats": False,
        "prizepool": None,
        "winner_id": None,
        "winner_type": None,
    }
    row = scrape.tournament_to_row(record)
    record["full_name"] = "IEM Katowice 2026 Group A"
    row = scrape.tournament_to_row(record)
    assert "full_name" not in row


@pytest.mark.parametrize(
    ("streams_list", "expected_url"),
    [
        (
            [
                {"main": False, "official": True, "raw_url": "https://example.com/official"},
                {"main": True, "official": True, "raw_url": "https://example.com/main-official"},
                {"main": True, "official": True, "raw_url": "https://example.com/second-main-official"},
            ],
            "https://example.com/main-official",
        ),
        (
            [
                {"main": True, "official": True, "raw_url": "  "},
                {"main": False, "official": True, "raw_url": "https://example.com/official"},
                {"main": True, "official": False, "raw_url": "https://example.com/main"},
            ],
            "https://example.com/official",
        ),
        (
            [
                {"main": True, "official": True, "raw_url": None},
                {"main": True, "official": False, "raw_url": "https://example.com/main"},
            ],
            "https://example.com/main",
        ),
        ([{"main": False, "official": False, "raw_url": "https://example.com/neither"}], None),
        ([{"main": True, "official": True, "raw_url": ""}], None),
        ([], None),
        (None, None),
    ],
)
def test_match_to_row_maps_stream_raw_url_by_priority(streams_list, expected_url):
    row = scrape.match_to_row({"id": 1, "streams_list": streams_list})

    assert row["streams_raw_url"] == expected_url
    assert "complete" not in row


def test_videogame_to_row_current_version_can_be_none():
    """
    current_version is None for most games (e.g. Counter-Strike, Dota 2).
    Only LoL and Valorant currently return a non-None version.
    Both cases must be mapped correctly - this is not a bug.
    """
    row_none = scrape.videogame_to_row(
        {"id": 3, "name": "Counter-Strike", "slug": "cs-go", "current_version": None}
    )
    assert row_none["current_version"] is None

    row_versioned = scrape.videogame_to_row(
        {"id": 1, "name": "LoL", "slug": "lol", "current_version": "16.13.1"}
    )
    assert row_versioned["current_version"] == "16.13.1"


def test_match_opponent_rows_falls_back_to_slot_type_when_opponent_has_no_type():
    """
    When the nested opponent dict has no 'type' key, the row must fall back
    to the slot-level 'type' field without altering the API casing.
    """
    record = {
        "id": 5,
        "winner_id": None,
        "opponents": [
            {"type": "Player", "opponent": {"id": 99}},
        ],
        "results": [],
    }
    rows = scrape.match_opponent_rows(record)
    assert len(rows) == 1
    assert rows[0]["opponent_type"] == "Player"


def test_parse_since_strips_leading_trailing_whitespace():
    """Leading/trailing whitespace must not break the shorthand parser."""
    from datetime import datetime, timedelta, timezone

    result = scrape._parse_since("  24h  ")
    parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    expected = datetime.now(timezone.utc) - timedelta(hours=24)
    assert abs((parsed - expected).total_seconds()) < 2


def test_canned_queries_execute_with_blank_and_representative_parameters():
    """Every canned query has valid SQL and accepts blank and representative filters."""
    metadata_path = Path(__file__).parent / "data" / "metadata.json"
    queries = json.loads(metadata_path.read_text())["databases"]["esports"]["queries"]
    connection = _metadata_fixture_connection()
    try:
        for query in queries.values():
            blank_params = {name: "" for name in query.get("params", [])}
            representative_params = {
                name: {
                    "game": "Test Game",
                    "year": "2025",
                    "tier": "s",
                    "tournament": "Test",
                    "team": "Test",
                }[name]
                for name in query.get("params", [])
            }
            assert connection.execute(query["sql"], blank_params).description
            connection.execute(query["sql"], representative_params).fetchall()
    finally:
        connection.close()


def test_team_queries_return_bounded_defaults_and_apply_partial_filters():
    metadata_path = Path(__file__).parent / "data" / "metadata.json"
    queries = json.loads(metadata_path.read_text())["databases"]["esports"]["queries"]
    connection = _metadata_fixture_connection()
    try:
        roster_query = queries["team_roster"]["sql"]
        roster_all = connection.execute(roster_query, {"game": "", "team": ""}).fetchall()
        assert [row[3] for row in roster_all] == ["Active Player", "Other Player"]
        assert len(roster_all) <= 100
        assert [row[3] for row in connection.execute(
            roster_query, {"game": "Test Game", "team": ""}
        ).fetchall()] == ["Active Player"]
        assert [row[3] for row in connection.execute(
            roster_query, {"game": "", "team": "Other"}
        ).fetchall()] == ["Other Player"]
        assert [row[3] for row in connection.execute(
            roster_query, {"game": "Other Game", "team": "Other"}
        ).fetchall()] == ["Other Player"]
        assert connection.execute(
            roster_query, {"game": "Missing Game", "team": ""}
        ).fetchall() == []

        schedule_query = queries["team_schedule"]["sql"]
        schedule_cursor = connection.execute(
            schedule_query, {"game": "", "team": ""}
        )
        schedule_columns = [column[0] for column in schedule_cursor.description]
        match_id_index = schedule_columns.index("match_id")
        stream_url_index = schedule_columns.index("streams_raw_url")
        schedule_all = schedule_cursor.fetchall()
        assert len(schedule_all) == 4
        scheduled_with_url = next(row for row in schedule_all if row[match_id_index] == 2)
        scheduled_without_url = next(row for row in schedule_all if row[match_id_index] == 1)
        assert scheduled_with_url[stream_url_index] == "https://www.twitch.tv/test-stream"
        assert scheduled_without_url[stream_url_index] is None
        assert len(schedule_all) <= 100
        game_schedule = connection.execute(
            schedule_query, {"game": "Other Game", "team": ""}
        ).fetchall()
        assert len(game_schedule) == 1
        team_schedule = connection.execute(
            schedule_query, {"game": "", "team": "Test Team"}
        ).fetchall()
        assert {row[match_id_index] for row in team_schedule} == {1, 2, 3}
        combined_schedule = connection.execute(
            schedule_query, {"game": "Test Game", "team": "Test Team"}
        ).fetchall()
        assert {row[match_id_index] for row in combined_schedule} == {1, 2, 3}
        assert connection.execute(
            schedule_query, {"game": "Missing Game", "team": ""}
        ).fetchall() == []
    finally:
        connection.close()


def test_match_lifecycle_filters_narrow_results_and_blank_values_broaden_them():
    metadata_path = Path(__file__).parent / "data" / "metadata.json"
    queries = json.loads(metadata_path.read_text())["databases"]["esports"]["queries"]
    connection = _metadata_fixture_connection()
    try:
        completed_query = queries["completed_matches"]["sql"]
        all_completed = connection.execute(
            completed_query, {"game": "", "tier": "", "tournament": ""}
        ).fetchall()
        assert len(all_completed) == 3
        assert len(connection.execute(
            completed_query, {"game": "Test Game", "tier": "", "tournament": ""}
        ).fetchall()) == 2
        assert len(connection.execute(
            completed_query, {"game": "", "tier": "s", "tournament": ""}
        ).fetchall()) == 2
        assert len(connection.execute(
            completed_query, {"game": "", "tier": "", "tournament": "Other"}
        ).fetchall()) == 1

        live_params = {"game": "", "tier": "", "tournament": ""}
        live_cursor = connection.execute(queries["live_matches"]["sql"], live_params)
        assert len(live_cursor.fetchall()) == 1
        assert "streams_raw_url" in [column[0] for column in live_cursor.description]
        upcoming_params = {"game": "", "tier": "", "tournament": ""}
        upcoming_cursor = connection.execute(
            queries["upcoming_matches"]["sql"], upcoming_params
        )
        upcoming_columns = [column[0] for column in upcoming_cursor.description]
        upcoming_rows = [dict(zip(upcoming_columns, row)) for row in upcoming_cursor.fetchall()]
        assert len(upcoming_rows) == 1
        assert upcoming_rows[0]["streams_raw_url"] == "https://www.twitch.tv/test-stream"
    finally:
        connection.close()


def test_tournament_query_context_order_and_lifecycle_descriptions():
    metadata_path = Path(__file__).parent / "data" / "metadata.json"
    queries = json.loads(metadata_path.read_text())["databases"]["esports"]["queries"]
    connection = _metadata_fixture_connection()
    try:
        expected_columns = ["game", "league", "series", "tournament"]
        for query_name in (
            "major_events",
            "ongoing_tournaments",
            "upcoming_tournaments",
            "completed_tournaments",
        ):
            query = queries[query_name]
            cursor = connection.execute(
                query["sql"], {name: "" for name in query.get("params", [])}
            )
            assert [column[0] for column in cursor.description[:4]] == expected_columns

        major_rows = connection.execute(
            queries["major_events"]["sql"], {"game": ""}
        ).fetchall()
        assert any(row[2] == "Test Series Full Name" and row[3] == "Test Cup" for row in major_rows)
        assert any(row[2] is None and row[3] == "No Series Completed" for row in major_rows)

        ongoing = queries["ongoing_tournaments"]
        assert ongoing["title"] == "Ongoing Tournaments"
        ongoing_rows = connection.execute(
            ongoing["sql"], {"game": "", "tier": ""}
        ).fetchall()
        assert any(row[3] == "No Series Ongoing" and row[2] is None for row in ongoing_rows)
        assert "dates" in ongoing["description"].lower()
        assert "separate from whether any match is live" in ongoing["description"]
        assert "datetime('now')" in ongoing["sql"]

        upcoming_rows = connection.execute(
            queries["upcoming_tournaments"]["sql"], {"game": "", "tier": ""}
        ).fetchall()
        assert any(row[3] == "No Series Upcoming" and row[2] is None for row in upcoming_rows)
        completed_rows = connection.execute(
            queries["completed_tournaments"]["sql"], {"game": "", "tier": ""}
        ).fetchall()
        assert any(row[3] == "No Series Completed" and row[2] is None for row in completed_rows)

        live = queries["live_matches"]
        assert live["title"] == "Live Matches"
        assert "running" in live["description"].lower()
        assert "direct watch url" in live["description"].lower()
        assert "m.status = 'running'" in live["sql"]
    finally:
        connection.close()


def test_match_query_context_order_full_names_and_missing_series():
    metadata_path = Path(__file__).parent / "data" / "metadata.json"
    queries = json.loads(metadata_path.read_text())["databases"]["esports"]["queries"]
    connection = _metadata_fixture_connection()
    try:
        expected_columns = ["game", "league", "series", "tournament"]
        query_names = (
            "live_matches",
            "upcoming_matches",
            "completed_matches",
            "team_schedule",
        )
        for query_name in query_names:
            query = queries[query_name]
            params = {name: "" for name in query.get("params", [])}
            cursor = connection.execute(query["sql"], params)
            columns = [column[0] for column in cursor.description]
            assert columns[:4] == expected_columns
            if query_name == "completed_matches":
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                missing_series = next(row for row in rows if row["match_name"] == "No Series Finished")
                assert missing_series["series"] is None
                assert missing_series["tournament"] == "No Series Completed"
                missing_full_name = next(row for row in rows if row["match_name"] == "Other Finished")
                assert missing_full_name["series"] is None
            elif query_name == "live_matches":
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                assert rows[0]["series"] == "Test Series Full Name"
            elif query_name == "upcoming_matches":
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                assert any(
                    row["match_name"] == "Test Upcoming"
                    and row["series"] == "Test Series Full Name"
                    for row in rows
                )

        live = queries["live_matches"]
        assert "status is marked running" in live["description"].lower()
        assert "different from a tournament being ongoing" in live["description"].lower()
        assert "m.status = 'running'" in live["sql"]
    finally:
        connection.close()


def test_leagues_by_game_all_games_default_is_bounded_and_ordered():
    metadata_path = Path(__file__).parent / "data" / "metadata.json"
    database_path = Path(__file__).parent / "data" / "esports.db"
    if not database_path.is_file():
        pytest.skip("local esports.db snapshot is unavailable")
    query = json.loads(metadata_path.read_text())["databases"]["esports"]["queries"]["leagues_by_game"]
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        rows = connection.execute(query["sql"], {"game": ""}).fetchall()
        assert len(rows) == 100
        assert [row[3] for row in rows] == sorted(
            (row[3] for row in rows), reverse=True
        )
    finally:
        connection.close()


def test_team_schedule_performance_thresholds_on_local_snapshot():
    """Team schedule remains within its warm-snapshot budgets with game filtering."""
    metadata_path = Path(__file__).parent / "data" / "metadata.json"
    database_path = Path(__file__).parent / "data" / "esports.db"
    if not database_path.is_file():
        pytest.skip("local esports.db snapshot is unavailable")
    query = json.loads(metadata_path.read_text())["databases"]["esports"]["queries"]["team_schedule"]["sql"]
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        workloads = (
            ({"team": "Liquid", "game": ""}, 250),
            ({"team": "", "game": ""}, 500),
            ({"team": "", "game": "Counter-Strike"}, 500),
        )
        for params, threshold_ms in workloads:
            durations = []
            rows = []
            for _ in range(5):
                started = perf_counter()
                rows = connection.execute(query, params).fetchall()
                durations.append((perf_counter() - started) * 1000)
            assert len(rows) <= 100
            assert median(durations) < threshold_ms
    finally:
        connection.close()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
