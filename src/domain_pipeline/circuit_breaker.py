"""Circuit breaker for external API calls.

Trips after consecutive failures, preventing cascading failures
by short-circuiting calls to broken services. Automatically
retries after a cooldown period.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is attempted on an open circuit."""
    def __init__(self, name: str, remaining_seconds: float):
        self.name = name
        self.remaining_seconds = remaining_seconds
        super().__init__(f"Circuit '{name}' is open, retry in {remaining_seconds:.0f}s")


@dataclass
class CircuitBreaker:
    """Thread-safe circuit breaker.

    Args:
        name: Identifier for this circuit (e.g. "searxng", "google_places").
        failure_threshold: Number of consecutive failures before opening.
        recovery_timeout: Seconds to wait before trying half-open.
    """
    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._current_state()

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute fn if circuit allows, tracking success/failure."""
        state = self.state
        if state == CircuitState.OPEN:
            remaining = self.recovery_timeout - (time.monotonic() - self._last_failure_time)
            raise CircuitOpenError(self.name, max(remaining, 0))

        try:
            result = fn(*args, **kwargs)
            self.record_success()
            return result
        except Exception:
            self.record_failure()
            raise

    def _current_state(self) -> CircuitState:
        """Return current state (must be called with _lock held)."""
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state

    def status(self) -> dict:
        """Return status dict for health endpoint."""
        with self._lock:
            return {
                "state": self._current_state().value,
                "failure_count": self._failure_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
            }


# --- Global circuit breaker registry ---
_registry: dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()


def get_breaker(name: str, failure_threshold: int = 5, recovery_timeout: float = 60.0) -> CircuitBreaker:
    """Get or create a circuit breaker by name."""
    with _registry_lock:
        if name not in _registry:
            _registry[name] = CircuitBreaker(
                name=name,
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
            )
        return _registry[name]


def all_breaker_statuses() -> dict[str, dict]:
    """Return status of all registered circuit breakers."""
    with _registry_lock:
        return {name: cb.status() for name, cb in _registry.items()}
