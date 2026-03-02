"""Unit tests for domain_pipeline.circuit_breaker module."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

import domain_pipeline.circuit_breaker as cb_module
from domain_pipeline.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    all_breaker_statuses,
    get_breaker,
)


@pytest.fixture(autouse=True)
def _clear_registry():
    """Ensure the global registry is empty before and after each test."""
    cb_module._registry.clear()
    yield
    cb_module._registry.clear()


# ---------- CircuitState enum ----------


class TestCircuitState:
    def test_closed_value(self):
        assert CircuitState.CLOSED.value == "closed"

    def test_open_value(self):
        assert CircuitState.OPEN.value == "open"

    def test_half_open_value(self):
        assert CircuitState.HALF_OPEN.value == "half_open"


# ---------- CircuitOpenError ----------


class TestCircuitOpenError:
    def test_attributes(self):
        err = CircuitOpenError("searxng", 30.0)
        assert err.name == "searxng"
        assert err.remaining_seconds == 30.0

    def test_message(self):
        err = CircuitOpenError("searxng", 30.0)
        assert "searxng" in str(err)
        assert "30" in str(err)


# ---------- New breaker defaults ----------


class TestNewBreaker:
    def test_starts_closed(self):
        """1. New breaker starts in CLOSED state."""
        cb = CircuitBreaker(name="test")
        assert cb.state == CircuitState.CLOSED

    def test_default_failure_threshold(self):
        cb = CircuitBreaker(name="test")
        assert cb.failure_threshold == 5

    def test_default_recovery_timeout(self):
        cb = CircuitBreaker(name="test")
        assert cb.recovery_timeout == 60.0

    def test_custom_threshold_and_timeout(self):
        cb = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=30.0)
        assert cb.failure_threshold == 3
        assert cb.recovery_timeout == 30.0


# ---------- Success keeps circuit closed ----------


class TestRecordSuccess:
    def test_success_keeps_closed(self):
        """2. Success keeps circuit closed."""
        cb = CircuitBreaker(name="test")
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(name="test", failure_threshold=5)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb._failure_count == 0
        assert cb.state == CircuitState.CLOSED


# ---------- Failures below threshold ----------


class TestFailuresBelowThreshold:
    def test_failures_below_threshold_stay_closed(self):
        """3. Failures below threshold stay closed."""
        cb = CircuitBreaker(name="test", failure_threshold=5)
        for _ in range(4):
            cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 4


# ---------- Reaching threshold opens circuit ----------


class TestReachingThreshold:
    def test_reaching_threshold_opens_circuit(self):
        """4. Reaching failure_threshold opens circuit."""
        cb = CircuitBreaker(name="test", failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb._state == CircuitState.OPEN

    def test_exceeding_threshold_stays_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        for _ in range(5):
            cb.record_failure()
        assert cb._state == CircuitState.OPEN


# ---------- Open circuit raises CircuitOpenError ----------


class TestOpenCircuitBlocks:
    def test_open_circuit_raises_error(self):
        """5. Open circuit raises CircuitOpenError."""
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60.0)
        cb.record_failure()
        cb.record_failure()
        with pytest.raises(CircuitOpenError) as exc_info:
            cb.call(lambda: "ok")
        assert exc_info.value.name == "test"
        assert exc_info.value.remaining_seconds >= 0


# ---------- Recovery timeout -> HALF_OPEN ----------


class TestRecoveryTimeout:
    def test_after_timeout_becomes_half_open(self):
        """6. After recovery_timeout, circuit moves to HALF_OPEN."""
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60.0)

        with patch("domain_pipeline.circuit_breaker.time") as mock_time:
            # Record failures to open the circuit at time 100
            mock_time.monotonic.return_value = 100.0
            cb.record_failure()
            cb.record_failure()
            assert cb._state == CircuitState.OPEN

            # Before timeout: should still be OPEN
            mock_time.monotonic.return_value = 150.0
            assert cb.state == CircuitState.OPEN

            # After timeout: should transition to HALF_OPEN
            mock_time.monotonic.return_value = 161.0
            assert cb.state == CircuitState.HALF_OPEN

    def test_exactly_at_timeout_becomes_half_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60.0)

        with patch("domain_pipeline.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 100.0
            cb.record_failure()
            cb.record_failure()

            # Exactly at recovery_timeout boundary
            mock_time.monotonic.return_value = 160.0
            assert cb.state == CircuitState.HALF_OPEN


# ---------- Success in HALF_OPEN closes circuit ----------


class TestHalfOpenSuccess:
    def test_success_in_half_open_closes(self):
        """7. Success in HALF_OPEN closes circuit."""
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60.0)

        with patch("domain_pipeline.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 100.0
            cb.record_failure()
            cb.record_failure()

            # Advance past recovery timeout
            mock_time.monotonic.return_value = 161.0
            assert cb.state == CircuitState.HALF_OPEN

            # Record success
            cb.record_success()
            assert cb.state == CircuitState.CLOSED
            assert cb._failure_count == 0


# ---------- Failure in HALF_OPEN reopens circuit ----------


class TestHalfOpenFailure:
    def test_failure_in_half_open_reopens(self):
        """8. Failure in HALF_OPEN reopens circuit."""
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60.0)

        with patch("domain_pipeline.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 100.0
            cb.record_failure()
            cb.record_failure()

            # Advance past recovery timeout
            mock_time.monotonic.return_value = 161.0
            assert cb.state == CircuitState.HALF_OPEN

            # Record failure -- should re-open
            mock_time.monotonic.return_value = 162.0
            cb.record_failure()
            # The state internally is OPEN because failure_count >= threshold
            assert cb._state == CircuitState.OPEN


# ---------- call() method ----------


class TestCallMethod:
    def test_call_tracks_success(self):
        """9. call() method tracks success."""
        cb = CircuitBreaker(name="test")
        result = cb.call(lambda: 42)
        assert result == 42
        assert cb._failure_count == 0
        assert cb.state == CircuitState.CLOSED

    def test_call_tracks_failure(self):
        """10. call() method tracks failure."""
        cb = CircuitBreaker(name="test")

        def failing():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            cb.call(failing)
        assert cb._failure_count == 1

    def test_call_passes_args_and_kwargs(self):
        cb = CircuitBreaker(name="test")

        def adder(a, b, extra=0):
            return a + b + extra

        result = cb.call(adder, 1, 2, extra=10)
        assert result == 13

    def test_call_on_open_circuit_does_not_invoke_fn(self):
        cb = CircuitBreaker(name="test", failure_threshold=1)
        cb.record_failure()  # opens immediately
        fn = MagicMock()
        with pytest.raises(CircuitOpenError):
            cb.call(fn)
        fn.assert_not_called()

    def test_call_success_after_half_open(self):
        """call() in HALF_OPEN with a passing fn should close the circuit."""
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=10.0)

        with patch("domain_pipeline.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 0.0
            cb.record_failure()

            # Advance past recovery
            mock_time.monotonic.return_value = 11.0
            assert cb.state == CircuitState.HALF_OPEN

            result = cb.call(lambda: "recovered")
            assert result == "recovered"
            assert cb.state == CircuitState.CLOSED

    def test_call_failure_after_half_open_reopens(self):
        """call() in HALF_OPEN with a failing fn should re-open."""
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=10.0)

        with patch("domain_pipeline.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 0.0
            cb.record_failure()

            # Advance past recovery
            mock_time.monotonic.return_value = 11.0
            assert cb.state == CircuitState.HALF_OPEN

            with pytest.raises(RuntimeError):
                cb.call(lambda: (_ for _ in ()).throw(RuntimeError("fail")))
            # After exception in call, failure is recorded
            assert cb._state == CircuitState.OPEN


# ---------- status() method ----------


class TestStatusMethod:
    def test_status_returns_correct_dict(self):
        """11. status() returns correct dict."""
        cb = CircuitBreaker(name="test", failure_threshold=5, recovery_timeout=60.0)
        s = cb.status()
        assert s["state"] == "closed"
        assert s["failure_count"] == 0
        assert s["failure_threshold"] == 5
        assert s["recovery_timeout"] == 60.0

    def test_status_reflects_failures(self):
        cb = CircuitBreaker(name="test", failure_threshold=5)
        cb.record_failure()
        cb.record_failure()
        s = cb.status()
        assert s["failure_count"] == 2
        assert s["state"] == "closed"

    def test_status_reflects_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        s = cb.status()
        assert s["state"] == "open"


# ---------- get_breaker() registry ----------


class TestGetBreaker:
    def test_returns_same_instance_for_same_name(self):
        """12. get_breaker() returns same instance for same name."""
        b1 = get_breaker("searxng")
        b2 = get_breaker("searxng")
        assert b1 is b2

    def test_returns_different_instances_for_different_names(self):
        b1 = get_breaker("searxng")
        b2 = get_breaker("google_places")
        assert b1 is not b2
        assert b1.name == "searxng"
        assert b2.name == "google_places"

    def test_custom_threshold_on_creation(self):
        b = get_breaker("custom", failure_threshold=10, recovery_timeout=120.0)
        assert b.failure_threshold == 10
        assert b.recovery_timeout == 120.0

    def test_second_call_ignores_new_params(self):
        """Once created, parameters are not updated on subsequent calls."""
        b1 = get_breaker("api", failure_threshold=3)
        b2 = get_breaker("api", failure_threshold=99)
        assert b2.failure_threshold == 3  # original value
        assert b1 is b2


# ---------- all_breaker_statuses() ----------


class TestAllBreakerStatuses:
    def test_includes_all_registered_breakers(self):
        """13. all_breaker_statuses() includes all registered breakers."""
        get_breaker("alpha")
        get_breaker("beta")
        get_breaker("gamma")
        statuses = all_breaker_statuses()
        assert set(statuses.keys()) == {"alpha", "beta", "gamma"}
        for name, s in statuses.items():
            assert "state" in s
            assert "failure_count" in s

    def test_empty_registry(self):
        statuses = all_breaker_statuses()
        assert statuses == {}

    def test_statuses_reflect_circuit_state(self):
        b = get_breaker("failing", failure_threshold=2)
        b.record_failure()
        b.record_failure()
        statuses = all_breaker_statuses()
        assert statuses["failing"]["state"] == "open"
        assert statuses["failing"]["failure_count"] == 2
