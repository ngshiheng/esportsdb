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

import scrape as main


def make_client(since=None, page_size=3, page_delay=0.0):
    """Return a PandaScoreClient with all HTTP internals mocked out."""
    with patch("scrape.SyncSqliteStorage"), patch("scrape.SyncCacheClient"):
        return main.PandaScoreClient(
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
        client, "_fetch_page_with_retry", return_value=page1
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
        client, "_fetch_page_with_retry", return_value=page1
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

    with patch.object(client, "_fetch_page_with_retry", return_value=page1):
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
        client, "_fetch_page_with_retry", side_effect=[full_page, partial_page]
    ) as mock_fetch:
        results = list(client.fetch_all("leagues"))

    assert results == [full_page, partial_page]
    assert mock_fetch.call_count == 2


def test_match_opponent_rows_basic():
    """
    Two opponents with a declared winner: the winning opponent gets is_winner=1,
    the loser gets is_winner=0. score and opponent_type are extracted correctly.
    """
    record = {
        "id": 42,
        "winner_id": 10,
        "opponents": [
            {"opponent": {"id": 10, "type": "Team"}, "score": 2},
            {"opponent": {"id": 20, "type": "Team"}, "score": 0},
        ],
    }

    rows = main.match_opponent_rows(record)

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
            {"opponent": {"id": 5, "type": "Team"}, "score": 1},
            {"score": 0},  # no 'opponent' key
            {"opponent": None, "score": 0},  # opponent is None
        ],
    }

    rows = main.match_opponent_rows(record)

    assert len(rows) == 1
    assert rows[0]["opponent_id"] == 5


def test_match_opponent_rows_no_winner_sets_is_winner_none():
    """
    When winner_id is None (match not yet finished), is_winner must be None
    for every row — not 0 — because 0 implies a known loser.
    """
    record = {
        "id": 7,
        "winner_id": None,
        "opponents": [
            {"opponent": {"id": 1, "type": "Team"}, "score": None},
            {"opponent": {"id": 2, "type": "Team"}, "score": None},
        ],
    }

    rows = main.match_opponent_rows(record)

    assert all(row["is_winner"] is None for row in rows)


def test_parse_since_hours():
    """'48h' should produce an ISO-8601 UTC string ~48 hours in the past."""
    from datetime import datetime, timedelta, timezone

    result = main._parse_since("48h")
    parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    expected = datetime.now(timezone.utc) - timedelta(hours=48)

    # _parse_since truncates to whole seconds; allow ±2s tolerance
    assert abs((parsed - expected).total_seconds()) < 2


def test_parse_since_days():
    """'7d' should produce an ISO-8601 UTC string ~7 days in the past."""
    from datetime import datetime, timedelta, timezone

    result = main._parse_since("7d")
    parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    expected = datetime.now(timezone.utc) - timedelta(days=7)

    assert abs((parsed - expected).total_seconds()) < 2


def test_parse_since_passthrough_iso8601():
    """An already-valid ISO-8601 string must be returned unchanged."""
    iso = "2025-03-15T12:00:00Z"
    assert main._parse_since(iso) == iso


# ---------------------------------------------------------------------------
# scrape_resource  [HIGH RISK — silent skip + wrong total count]
# ---------------------------------------------------------------------------


def test_scrape_resource_skip_fk_errors_continues_and_still_counts_full_page():
    """
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
        total = main.scrape_resource(
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
            main.scrape_resource(
                client=client,
                db=mock_db,
                endpoint="teams",
                to_row=lambda r: r,
                table="teams",
                skip_fk_errors=False,
            )


def test_nested_id_extracts_id_from_dict():
    assert main._nested_id({"videogame": {"id": 7, "name": "CS2"}}, "videogame") == 7


def test_nested_id_returns_none_when_key_absent():
    assert main._nested_id({}, "videogame") is None


def test_nested_id_returns_none_when_value_is_not_dict():
    # API occasionally returns a scalar or null for nested objects
    assert main._nested_id({"videogame": None}, "videogame") is None
    assert main._nested_id({"videogame": 42}, "videogame") is None


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
