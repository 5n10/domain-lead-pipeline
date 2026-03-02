"""Unit tests for domain_pipeline.metrics module."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from domain_pipeline.metrics import collect_metrics


def _make_mock_session():
    """Create a mock session that simulates all database queries in collect_metrics().

    Returns the mock session with chained execute().first()/all()/scalar() responses.
    """
    mock_session = MagicMock()

    # Track call count to return different results for each session.execute() call
    call_results = []

    # 1. business_totals: (total, no_website, scored, no_website_scored)
    business_totals_result = MagicMock()
    business_totals_result.__getitem__ = lambda self, i: [100, 40, 60, 30][i]
    business_totals_result.first = MagicMock(return_value=business_totals_result)
    call_results.append(business_totals_result)

    # 2. domain_status_rows: list of (status, count)
    domain_status_result = MagicMock()
    domain_status_result.all.return_value = [("active", 50), ("parked", 20)]
    call_results.append(domain_status_result)

    # 3. contact_totals: (total, scored)
    contact_totals_result = MagicMock()
    contact_totals_result.__getitem__ = lambda self, i: [80, 50][i]
    contact_totals_result.first = MagicMock(return_value=contact_totals_result)
    call_results.append(contact_totals_result)

    # 4. export_totals: (total, queued)
    export_totals_result = MagicMock()
    export_totals_result.__getitem__ = lambda self, i: [10, 3][i]
    export_totals_result.first = MagicMock(return_value=export_totals_result)
    call_results.append(export_totals_result)

    # 5. business_export_totals: (total, queued)
    business_export_totals_result = MagicMock()
    business_export_totals_result.__getitem__ = lambda self, i: [25, 5][i]
    business_export_totals_result.first = MagicMock(return_value=business_export_totals_result)
    call_results.append(business_export_totals_result)

    # 6-12. verification_counts: one scalar per VERIFICATION_KEY (7 keys)
    # Keys: llm_verified, domain_guess_verified, ddg_verified,
    #        google_places_verified, foursquare_verified, google_search_verified, searxng_verified
    verification_values = [30, 10, 40, 20, 15, 5, 8]
    for v in verification_values:
        r = MagicMock()
        r.scalar.return_value = v
        call_results.append(r)

    # 13. any_verified scalar
    any_verified_result = MagicMock()
    any_verified_result.scalar.return_value = 70
    call_results.append(any_verified_result)

    # 14. ddg_conclusive scalar
    ddg_conclusive_result = MagicMock()
    ddg_conclusive_result.scalar.return_value = 35
    call_results.append(ddg_conclusive_result)

    # 15. ddg_no_results scalar
    ddg_no_results_result = MagicMock()
    ddg_no_results_result.scalar.return_value = 5
    call_results.append(ddg_no_results_result)

    # 16. llm_conclusive scalar
    llm_conclusive_result = MagicMock()
    llm_conclusive_result.scalar.return_value = 25
    call_results.append(llm_conclusive_result)

    # 17. llm_not_sure scalar
    llm_not_sure_result = MagicMock()
    llm_not_sure_result.scalar.return_value = 5
    call_results.append(llm_not_sure_result)

    # 18. searxng_conclusive scalar
    searxng_conclusive_result = MagicMock()
    searxng_conclusive_result.scalar.return_value = 6
    call_results.append(searxng_conclusive_result)

    # 19. searxng_no_results scalar
    searxng_no_results_result = MagicMock()
    searxng_no_results_result.scalar.return_value = 2
    call_results.append(searxng_no_results_result)

    # 20. confidence_dist rows
    conf_rows_result = MagicMock()
    conf_rows_result.all.return_value = [
        ("high", 20),
        ("medium", 15),
        ("low", 5),
        ("unverified", 20),
    ]
    call_results.append(conf_rows_result)

    # 21. queue_rows
    queue_rows_result = MagicMock()
    queue_rows_result.all.return_value = [
        ("ddg", "pending", 10),
        ("ddg", "done", 5),
        ("llm", "pending", 3),
    ]
    call_results.append(queue_rows_result)

    # 22. recent_jobs
    started = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    finished = datetime(2025, 1, 15, 10, 5, 0, tzinfo=timezone.utc)
    recent_jobs_result = MagicMock()
    recent_jobs_result.all.return_value = [
        ("import_osm", "completed", started, finished, 50),
    ]
    call_results.append(recent_jobs_result)

    # Wire up side_effect: some calls use .first(), some .all(), some .scalar()
    # The session.execute() returns the result objects in order
    mock_session.execute.side_effect = call_results

    return mock_session


class TestCollectMetrics:
    @patch("domain_pipeline.metrics.session_scope")
    def test_returns_all_top_level_keys(self, mock_scope):
        """collect_metrics() returns a dict with all expected sections."""
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        expected_keys = {
            "businesses",
            "domains",
            "contacts",
            "exports",
            "business_exports",
            "verification",
            "verification_details",
            "confidence_distribution",
            "queue",
            "recent_jobs",
        }
        assert set(result.keys()) == expected_keys

    @patch("domain_pipeline.metrics.session_scope")
    def test_business_totals(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        biz = result["businesses"]
        assert biz["total"] == 100
        assert biz["no_website"] == 40
        assert biz["scored"] == 60
        assert biz["no_website_scored"] == 30
        assert biz["no_website_unscored"] == 10  # max(40 - 30, 0)

    @patch("domain_pipeline.metrics.session_scope")
    def test_domain_status(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        assert result["domains"] == {"active": 50, "parked": 20}

    @patch("domain_pipeline.metrics.session_scope")
    def test_contacts(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        contacts = result["contacts"]
        assert contacts["total"] == 80
        assert contacts["scored"] == 50
        assert contacts["unscored"] == 30  # max(80 - 50, 0)

    @patch("domain_pipeline.metrics.session_scope")
    def test_exports(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        assert result["exports"]["total"] == 10
        assert result["exports"]["queued"] == 3

    @patch("domain_pipeline.metrics.session_scope")
    def test_business_exports(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        assert result["business_exports"]["total"] == 25
        assert result["business_exports"]["queued"] == 5

    @patch("domain_pipeline.metrics.session_scope")
    def test_verification_counts(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        v = result["verification"]
        assert v["llm"] == 30
        assert v["domain_guess"] == 10
        assert v["ddg"] == 40
        assert v["google_places"] == 20
        assert v["foursquare"] == 15
        assert v["google_search"] == 5
        assert v["searxng"] == 8
        assert v["any_source"] == 70

    @patch("domain_pipeline.metrics.session_scope")
    def test_verification_details(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        d = result["verification_details"]
        assert d["ddg_conclusive"] == 35
        assert d["ddg_no_results"] == 5
        assert d["llm_conclusive"] == 25
        assert d["llm_not_sure"] == 5
        assert d["searxng_conclusive"] == 6
        assert d["searxng_no_results"] == 2

    @patch("domain_pipeline.metrics.session_scope")
    def test_confidence_distribution(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        conf = result["confidence_distribution"]
        assert conf["high"] == 20
        assert conf["medium"] == 15
        assert conf["low"] == 5
        assert conf["unverified"] == 20

    @patch("domain_pipeline.metrics.session_scope")
    def test_queue(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        q = result["queue"]
        assert "ddg" in q
        assert "llm" in q
        assert q["ddg"]["pending"] == 10
        assert q["ddg"]["done"] == 5
        assert q["llm"]["pending"] == 3

    @patch("domain_pipeline.metrics.session_scope")
    def test_recent_jobs(self, mock_scope):
        mock_session = _make_mock_session()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        jobs = result["recent_jobs"]
        assert len(jobs) == 1
        assert jobs[0]["job_name"] == "import_osm"
        assert jobs[0]["status"] == "completed"
        assert jobs[0]["processed_count"] == 50
        assert jobs[0]["started_at"] is not None
        assert jobs[0]["finished_at"] is not None


class TestCollectMetricsEdgeCases:
    @patch("domain_pipeline.metrics.session_scope")
    def test_null_counts_default_to_zero(self, mock_scope):
        """When database returns None for aggregate counts, they should be 0."""
        mock_session = MagicMock()

        call_results = []

        # business_totals with all None
        bt = MagicMock()
        bt.__getitem__ = lambda self, i: None
        bt.first = MagicMock(return_value=bt)
        call_results.append(bt)

        # domain_status_rows: empty
        ds = MagicMock()
        ds.all.return_value = []
        call_results.append(ds)

        # contact_totals: all None
        ct = MagicMock()
        ct.__getitem__ = lambda self, i: None
        ct.first = MagicMock(return_value=ct)
        call_results.append(ct)

        # export_totals: all None
        et = MagicMock()
        et.__getitem__ = lambda self, i: None
        et.first = MagicMock(return_value=et)
        call_results.append(et)

        # business_export_totals: all None
        bet = MagicMock()
        bet.__getitem__ = lambda self, i: None
        bet.first = MagicMock(return_value=bet)
        call_results.append(bet)

        # verification_counts: 7 keys returning 0
        for _ in range(7):
            r = MagicMock()
            r.scalar.return_value = 0
            call_results.append(r)

        # any_verified
        av = MagicMock()
        av.scalar.return_value = 0
        call_results.append(av)

        # ddg_conclusive, ddg_no_results, llm_conclusive, llm_not_sure, searxng_conclusive, searxng_no_results
        for _ in range(6):
            r = MagicMock()
            r.scalar.return_value = 0
            call_results.append(r)

        # confidence rows: empty
        cr = MagicMock()
        cr.all.return_value = []
        call_results.append(cr)

        # queue: empty
        qr = MagicMock()
        qr.all.return_value = []
        call_results.append(qr)

        # recent_jobs: empty
        rj = MagicMock()
        rj.all.return_value = []
        call_results.append(rj)

        mock_session.execute.side_effect = call_results

        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        assert result["businesses"]["total"] == 0
        assert result["businesses"]["no_website"] == 0
        assert result["businesses"]["scored"] == 0
        assert result["contacts"]["total"] == 0
        assert result["exports"]["total"] == 0
        assert result["business_exports"]["total"] == 0
        assert result["domains"] == {}
        assert result["queue"] == {}
        assert result["recent_jobs"] == []

    @patch("domain_pipeline.metrics.session_scope")
    def test_no_website_unscored_never_negative(self, mock_scope):
        """no_website_unscored uses max(..., 0) so it can't go negative."""
        mock_session = MagicMock()

        call_results = []

        # business_totals: no_website < no_website_scored (edge case)
        # total=10, no_website=5, scored=8, no_website_scored=7
        bt = MagicMock()
        bt.__getitem__ = lambda self, i: [10, 5, 8, 7][i]
        bt.first = MagicMock(return_value=bt)
        call_results.append(bt)

        # domain_status_rows: empty
        ds = MagicMock()
        ds.all.return_value = []
        call_results.append(ds)

        # contact_totals
        ct = MagicMock()
        ct.__getitem__ = lambda self, i: [0, 0][i]
        ct.first = MagicMock(return_value=ct)
        call_results.append(ct)

        # export_totals
        et = MagicMock()
        et.__getitem__ = lambda self, i: [0, 0][i]
        et.first = MagicMock(return_value=et)
        call_results.append(et)

        # business_export_totals
        bet = MagicMock()
        bet.__getitem__ = lambda self, i: [0, 0][i]
        bet.first = MagicMock(return_value=bet)
        call_results.append(bet)

        # verification (7 keys + any_verified)
        for _ in range(8):
            r = MagicMock()
            r.scalar.return_value = 0
            call_results.append(r)

        # verification details (6 scalars)
        for _ in range(6):
            r = MagicMock()
            r.scalar.return_value = 0
            call_results.append(r)

        # confidence
        cr = MagicMock()
        cr.all.return_value = []
        call_results.append(cr)

        # queue
        qr = MagicMock()
        qr.all.return_value = []
        call_results.append(qr)

        # recent_jobs
        rj = MagicMock()
        rj.all.return_value = []
        call_results.append(rj)

        mock_session.execute.side_effect = call_results

        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        # no_website_unscored = max(5 - 7, 0) = 0
        assert result["businesses"]["no_website_unscored"] == 0

    @patch("domain_pipeline.metrics.session_scope")
    def test_recent_jobs_with_null_timestamps(self, mock_scope):
        """Jobs with None timestamps should produce None in output."""
        mock_session = _make_mock_session()

        # Override the recent_jobs result to have None timestamps
        # We need to get to call index 21 (0-indexed) for recent_jobs
        original_side_effect = list(mock_session.execute.side_effect)
        rj = MagicMock()
        rj.all.return_value = [
            ("verify_batch", "running", None, None, 0),
        ]
        original_side_effect[-1] = rj
        mock_session.execute.side_effect = original_side_effect

        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        result = collect_metrics()

        jobs = result["recent_jobs"]
        assert len(jobs) == 1
        assert jobs[0]["started_at"] is None
        assert jobs[0]["finished_at"] is None
