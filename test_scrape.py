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
from unittest.mock import MagicMock, patch

import scrape


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
    # Page 2 must never be requested — the loop should have broken after page 1
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
    excluded from the yielded page. This confirms — and documents — the
    silent-drop behaviour so a future refactor doesn't change it unknowingly.
    """
    client = make_client(since="2025-01-10T00:00:00Z")

    page1 = [
        {"id": 1, "begin_at": "2025-01-12T00:00:00Z"},  # valid — included
        {"id": 2, "begin_at": None},  # None — silently dropped
        {"id": 3},  # key absent — silently dropped
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
    entirely — no row should be produced for it.
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
    for every row — not 0 — because 0 implies a known loser.
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


def test_scrape_resource_skip_fk_errors_continues_and_still_counts_full_page():
    """
    HIGH RISK: silent skip + wrong total count.

    When skip_fk_errors=True and one record raises IntegrityError, processing
    must continue for remaining records in the page. The returned total also
    counts the full page (documenting the known mis-count behaviour so any
    future fix is deliberate).
    """
    import sqlite3

    client = make_client()
    mock_db = MagicMock()

    records = [{"id": 1}, {"id": 2}, {"id": 3}]

    # upsert raises on the first record, succeeds for the others
    mock_db.upsert.side_effect = [
        sqlite3.IntegrityError("FK constraint failed"),
        None,
        None,
    ]

    with patch.object(client, "fetch_all", return_value=iter([records])):
        total = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="teams",
            to_row=lambda r: r,
            table="teams",
            skip_fk_errors=True,
        )

    # All three records were attempted despite the first failure
    assert mock_db.upsert.call_count == 3
    # Commit was called once (after the page)
    mock_db.commit.assert_called_once()
    # Total reflects full page length (documents the known mis-count)
    assert total == 3


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

    This exercises the `len(filtered) == len(records)` path — the break only
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
        total = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="matches",
            to_row=lambda r: r,
            table="matches",
        )

    assert total == 2
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
        total = scrape.scrape_resource(
            client=client,
            db=mock_db,
            endpoint="matches",
            to_row=lambda r: r,
            table="matches",
        )

    assert total == 1
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


def test_tournament_to_row_maps_full_name():
    """
    full_name is present in the PandaScore tournament response but is None
    for most tournaments (sparse data — not a code bug).
    The field must always be mapped regardless of its value.
    """
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
    assert "full_name" in row
    assert row["full_name"] is None

    # When the API does supply a full_name it must be stored.
    record["full_name"] = "IEM Katowice 2026 Group A"
    row = scrape.tournament_to_row(record)
    assert row["full_name"] == "IEM Katowice 2026 Group A"


def test_videogame_to_row_current_version_can_be_none():
    """
    current_version is None for most games (e.g. Counter-Strike, Dota 2).
    Only LoL and Valorant currently return a non-None version.
    Both cases must be mapped correctly — this is not a bug.
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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
