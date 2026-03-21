"""Unit tests for domain_pipeline.queue module."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from domain_pipeline.queue import (
    LAYER_ORDER,
    _uuid_to_lock_key,
    enqueue,
    claim_batch,
    complete,
    fail,
    requeue_failed,
    queue_depth,
)


# ---------- _uuid_to_lock_key ----------

class TestUuidToLockKey:
    def test_returns_two_ints(self):
        uid = uuid.uuid4()
        key1, key2 = _uuid_to_lock_key(uid)
        assert isinstance(key1, int)
        assert isinstance(key2, int)

    def test_deterministic(self):
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        assert _uuid_to_lock_key(uid) == _uuid_to_lock_key(uid)

    def test_different_uuids_different_keys(self):
        uid1 = uuid.UUID("00000000-0000-0000-0000-000000000001")
        uid2 = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
        assert _uuid_to_lock_key(uid1) != _uuid_to_lock_key(uid2)


# ---------- LAYER_ORDER ----------

class TestLayerOrder:
    def test_all_layers_present(self):
        expected = {"domain_guess", "searxng", "llm", "ddg", "google_search"}
        assert set(LAYER_ORDER.keys()) == expected

    def test_domain_guess_highest_priority(self):
        assert LAYER_ORDER["domain_guess"] == 0
        for layer, order in LAYER_ORDER.items():
            assert LAYER_ORDER["domain_guess"] <= order


# ---------- enqueue ----------

class TestEnqueue:
    def test_enqueues_new_businesses(self):
        session = MagicMock()
        # No existing items
        session.execute.return_value.all.return_value = []
        biz_ids = [uuid.uuid4(), uuid.uuid4()]

        count = enqueue(session, biz_ids, layer="domain_guess")

        assert count == 2
        session.add_all.assert_called_once()
        items = session.add_all.call_args[0][0]
        assert len(items) == 2

    def test_skips_already_queued(self):
        session = MagicMock()
        biz1 = uuid.uuid4()
        biz2 = uuid.uuid4()
        # biz1 already queued
        session.execute.return_value.all.return_value = [(biz1,)]

        count = enqueue(session, [biz1, biz2], layer="domain_guess")

        assert count == 1
        items = session.add_all.call_args[0][0]
        assert len(items) == 1
        assert items[0].business_id == biz2

    def test_empty_list_returns_zero(self):
        session = MagicMock()
        assert enqueue(session, [], layer="domain_guess") == 0

    def test_uses_layer_order_as_priority(self):
        session = MagicMock()
        session.execute.return_value.all.return_value = []
        biz_ids = [uuid.uuid4()]

        enqueue(session, biz_ids, layer="ddg")

        items = session.add_all.call_args[0][0]
        assert items[0].priority == LAYER_ORDER["ddg"]

    def test_custom_priority(self):
        session = MagicMock()
        session.execute.return_value.all.return_value = []
        biz_ids = [uuid.uuid4()]

        enqueue(session, biz_ids, layer="domain_guess", priority=99)

        items = session.add_all.call_args[0][0]
        assert items[0].priority == 99


# ---------- claim_batch ----------

class TestClaimBatch:
    def test_claims_unlocked_items(self):
        session = MagicMock()
        item = MagicMock()
        item.business_id = uuid.uuid4()
        item.status = "pending"
        item.attempts = 0

        session.execute.return_value.scalars.return_value.all.return_value = [item]
        # Advisory lock succeeds
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        session.execute.side_effect = [
            session.execute.return_value,  # First call: select candidates
            lock_result,  # Second call: advisory lock
        ]

        claimed = claim_batch(session, layer="domain_guess", limit=10, worker_id="w1")

        assert len(claimed) == 1
        assert claimed[0].status == "claimed"
        assert claimed[0].claimed_by == "w1"
        assert claimed[0].attempts == 1

    def test_skips_locked_items(self):
        session = MagicMock()
        item1 = MagicMock()
        item1.business_id = uuid.uuid4()
        item1.status = "pending"
        item1.attempts = 0
        item2 = MagicMock()
        item2.business_id = uuid.uuid4()
        item2.status = "pending"
        item2.attempts = 0

        session.execute.return_value.scalars.return_value.all.return_value = [item1, item2]

        # First advisory lock fails, second succeeds
        lock_fail = MagicMock()
        lock_fail.scalar.return_value = False
        lock_ok = MagicMock()
        lock_ok.scalar.return_value = True
        session.execute.side_effect = [
            session.execute.return_value,  # select candidates
            lock_fail,  # item1 lock
            lock_ok,  # item2 lock
        ]

        claimed = claim_batch(session, layer="domain_guess", limit=10)

        assert len(claimed) == 1
        assert claimed[0] == item2

    def test_respects_limit(self):
        session = MagicMock()
        items = []
        for _ in range(5):
            item = MagicMock()
            item.business_id = uuid.uuid4()
            item.status = "pending"
            item.attempts = 0
            items.append(item)

        session.execute.return_value.scalars.return_value.all.return_value = items

        lock_ok = MagicMock()
        lock_ok.scalar.return_value = True
        session.execute.side_effect = [
            session.execute.return_value,
            lock_ok, lock_ok,  # Only 2 locks needed for limit=2
        ]

        claimed = claim_batch(session, layer="domain_guess", limit=2)

        assert len(claimed) == 2

    def test_empty_when_no_candidates(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []

        claimed = claim_batch(session, layer="domain_guess", limit=10)

        assert claimed == []


# ---------- complete ----------

class TestComplete:
    def test_marks_completed(self):
        session = MagicMock()
        item_id = uuid.uuid4()

        complete(session, item_id)

        session.execute.assert_called_once()


# ---------- fail ----------

class TestFail:
    def test_marks_failed(self):
        session = MagicMock()
        item_id = uuid.uuid4()

        fail(session, item_id, "timeout")

        session.execute.assert_called_once()

    def test_truncates_long_error(self):
        session = MagicMock()
        item_id = uuid.uuid4()

        fail(session, item_id, "x" * 2000)

        session.execute.assert_called_once()


# ---------- requeue_failed ----------

class TestRequeueFailed:
    def test_requeues_failed_items(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 5

        count = requeue_failed(session)

        assert count == 5

    def test_filter_by_layer(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 2

        count = requeue_failed(session, layer="ddg")

        assert count == 2


# ---------- queue_depth ----------

class TestQueueDepth:
    def test_returns_depth_by_status(self):
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            ("pending", 10),
            ("claimed", 3),
            ("completed", 50),
        ]

        depth = queue_depth(session)

        assert depth == {"pending": 10, "claimed": 3, "completed": 50}

    def test_empty_queue(self):
        session = MagicMock()
        session.execute.return_value.all.return_value = []

        assert queue_depth(session) == {}
