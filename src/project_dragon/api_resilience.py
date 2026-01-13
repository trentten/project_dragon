from __future__ import annotations

import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Deque, Dict, Optional, TypeVar

T = TypeVar("T")


class RateLimitTimeout(TimeoutError):
    pass


class CircuitOpenError(RuntimeError):
    def __init__(self, account_id: int, state: str, cooldown_remaining_s: float, op_name: str) -> None:
        super().__init__(
            f"Circuit breaker is {state} for account_id={account_id} (cooldown {cooldown_remaining_s:.1f}s) op={op_name}"
        )
        self.account_id = account_id
        self.state = state
        self.cooldown_remaining_s = cooldown_remaining_s
        self.op_name = op_name


class RetryableAPIError(RuntimeError):
    """Generic wrapper when caller wants to force retry semantics."""


def _env_int(name: str, default: int) -> int:
    try:
        v = os.environ.get(name)
        if v is None:
            return int(default)
        return int(v)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        v = os.environ.get(name)
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreakerConfig:
    failure_window_s: float = 60.0
    open_after_failures: int = 5
    cooldown_s: float = 20.0


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_backoff_s: float = 0.25
    max_backoff_s: float = 2.0
    jitter_frac: float = 0.20


@dataclass
class RateLimitConfig:
    rate_per_s: float = 8.0
    burst: float = 16.0


class TokenBucket:
    def __init__(self, rate_per_s: float, burst: float) -> None:
        self.rate_per_s = max(0.001, float(rate_per_s))
        self.capacity = max(1.0, float(burst))
        self._tokens = self.capacity
        self._last_ts = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self._last_ts)
        self._last_ts = now
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_s)

    def acquire(self, *, cost: float = 1.0, timeout_s: float = 1.0) -> None:
        cost_f = max(0.0, float(cost))
        deadline = time.monotonic() + max(0.0, float(timeout_s))

        if cost_f <= 0:
            return

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= cost_f:
                    self._tokens -= cost_f
                    return
                needed = cost_f - self._tokens
                wait_s = needed / self.rate_per_s if self.rate_per_s > 0 else 999.0

            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                raise RateLimitTimeout(f"Token bucket timeout (cost={cost_f})")

            time.sleep(min(wait_s, remaining, 0.25))


class CircuitBreaker:
    def __init__(self, cfg: CircuitBreakerConfig) -> None:
        self.cfg = cfg
        self._state: Dict[int, CircuitState] = {}
        self._opened_at: Dict[int, float] = {}
        self._failures: Dict[int, Deque[float]] = {}
        self._half_open_probe_in_flight: Dict[int, bool] = {}
        self._lock = threading.Lock()

    def get_state(self, account_id: int) -> CircuitState:
        now = time.monotonic()
        with self._lock:
            st = self._state.get(account_id, CircuitState.CLOSED)
            if st == CircuitState.OPEN:
                opened_at = self._opened_at.get(account_id, now)
                if (now - opened_at) >= self.cfg.cooldown_s:
                    self._state[account_id] = CircuitState.HALF_OPEN
                    self._half_open_probe_in_flight[account_id] = False
                    return CircuitState.HALF_OPEN
            return self._state.get(account_id, CircuitState.CLOSED)

    def is_open(self, account_id: int) -> bool:
        return self.get_state(account_id) == CircuitState.OPEN

    def cooldown_remaining_s(self, account_id: int) -> float:
        now = time.monotonic()
        with self._lock:
            opened_at = self._opened_at.get(account_id)
            if opened_at is None:
                return 0.0
            rem = self.cfg.cooldown_s - (now - opened_at)
            return max(0.0, float(rem))

    def _prune_failures(self, account_id: int, now: float) -> None:
        q = self._failures.get(account_id)
        if q is None:
            return
        cutoff = now - self.cfg.failure_window_s
        while q and q[0] < cutoff:
            q.popleft()

    def record_failure(self, account_id: int, error_type: str = "unknown") -> None:
        now = time.monotonic()
        with self._lock:
            q = self._failures.setdefault(account_id, deque())
            q.append(now)
            self._prune_failures(account_id, now)

            st = self._state.get(account_id, CircuitState.CLOSED)
            if st == CircuitState.HALF_OPEN:
                # Probe failed -> reopen immediately
                self._state[account_id] = CircuitState.OPEN
                self._opened_at[account_id] = now
                self._half_open_probe_in_flight[account_id] = False
                return

            if len(q) >= int(self.cfg.open_after_failures):
                self._state[account_id] = CircuitState.OPEN
                self._opened_at[account_id] = now
                self._half_open_probe_in_flight[account_id] = False

    def record_success(self, account_id: int) -> None:
        now = time.monotonic()
        with self._lock:
            self._failures.pop(account_id, None)
            self._half_open_probe_in_flight[account_id] = False
            self._state[account_id] = CircuitState.CLOSED
            self._opened_at.pop(account_id, None)

    def allow_call(self, account_id: int) -> bool:
        now = time.monotonic()
        with self._lock:
            st = self._state.get(account_id, CircuitState.CLOSED)
            if st == CircuitState.CLOSED:
                return True
            if st == CircuitState.OPEN:
                opened_at = self._opened_at.get(account_id, now)
                if (now - opened_at) >= self.cfg.cooldown_s:
                    self._state[account_id] = CircuitState.HALF_OPEN
                    self._half_open_probe_in_flight[account_id] = False
                    st = CircuitState.HALF_OPEN
                else:
                    return False
            if st == CircuitState.HALF_OPEN:
                # Allow exactly one probe at a time.
                if self._half_open_probe_in_flight.get(account_id, False):
                    return False
                self._half_open_probe_in_flight[account_id] = True
                return True
            return True


@dataclass
class ResilienceConfig:
    rate_limit: RateLimitConfig
    retry: RetryConfig
    circuit: CircuitBreakerConfig


def default_config() -> ResilienceConfig:
    return ResilienceConfig(
        rate_limit=RateLimitConfig(
            rate_per_s=_env_float("DRAGON_WOOX_RATE_PER_S", 8.0),
            burst=_env_float("DRAGON_WOOX_RATE_BURST", 16.0),
        ),
        retry=RetryConfig(
            max_attempts=_env_int("DRAGON_WOOX_RETRY_MAX_ATTEMPTS", 3),
            base_backoff_s=_env_float("DRAGON_WOOX_RETRY_BASE_BACKOFF_S", 0.25),
            max_backoff_s=_env_float("DRAGON_WOOX_RETRY_MAX_BACKOFF_S", 2.0),
            jitter_frac=_env_float("DRAGON_WOOX_RETRY_JITTER_FRAC", 0.20),
        ),
        circuit=CircuitBreakerConfig(
            failure_window_s=_env_float("DRAGON_WOOX_CB_WINDOW_S", 60.0),
            open_after_failures=_env_int("DRAGON_WOOX_CB_OPEN_AFTER", 5),
            cooldown_s=_env_float("DRAGON_WOOX_CB_COOLDOWN_S", 20.0),
        ),
    )


def _backoff_s(cfg: RetryConfig, attempt_index: int) -> float:
    # attempt_index: 1..N-1 for retries
    raw = cfg.base_backoff_s * (2 ** max(0, attempt_index - 1))
    raw = min(float(cfg.max_backoff_s), float(raw))
    jitter = raw * float(cfg.jitter_frac) * (random.random() * 2.0 - 1.0)
    return max(0.0, raw + jitter)


def _looks_retryable_error(exc: BaseException) -> bool:
    # Keep simple: network-ish + explicit retry wrapper + common rate-limit/server signals.
    if isinstance(exc, RetryableAPIError):
        return True
    if isinstance(exc, TimeoutError):
        return True
    msg = str(exc).lower()
    if "429" in msg or "too many requests" in msg:
        return True
    if any(code in msg for code in (" 500", " 502", " 503", " 504")):
        return True
    if any(token in msg for token in ("timed out", "timeout", "temporar", "connection", "reset", "unavailable")):
        return True
    return False


class APIResilience:
    def __init__(self, cfg: Optional[ResilienceConfig] = None) -> None:
        self.cfg = cfg or default_config()
        self._buckets: Dict[int, TokenBucket] = {}
        self._bucket_lock = threading.Lock()
        self.circuit = CircuitBreaker(self.cfg.circuit)

    def _bucket(self, account_id: int) -> TokenBucket:
        with self._bucket_lock:
            b = self._buckets.get(account_id)
            if b is None:
                b = TokenBucket(self.cfg.rate_limit.rate_per_s, self.cfg.rate_limit.burst)
                self._buckets[account_id] = b
            return b

    def call_api(
        self,
        account_id: int,
        fn: Callable[[], T],
        *,
        op_name: str,
        retry: bool = True,
        cost: float = 1.0,
        timeout_s: float = 1.0,
        on_failure: Optional[Callable[[BaseException, int], None]] = None,
        on_success: Optional[Callable[[int], None]] = None,
    ) -> T:
        acct = int(account_id)

        if not self.circuit.allow_call(acct):
            raise CircuitOpenError(
                acct,
                str(self.circuit.get_state(acct).value),
                self.circuit.cooldown_remaining_s(acct),
                op_name,
            )

        self._bucket(acct).acquire(cost=cost, timeout_s=timeout_s)

        max_attempts = max(1, int(self.cfg.retry.max_attempts))
        attempts = max_attempts if retry else 1

        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                out = fn()
                self.circuit.record_success(acct)
                if on_success is not None:
                    try:
                        on_success(attempt)
                    except Exception:
                        pass
                return out
            except BaseException as exc:
                last_exc = exc
                retryable = _looks_retryable_error(exc)
                self.circuit.record_failure(acct, error_type=type(exc).__name__)
                if on_failure is not None:
                    try:
                        on_failure(exc, attempt)
                    except Exception:
                        pass

                if (not retry) or (attempt >= attempts) or (not retryable):
                    raise

                sleep_s = _backoff_s(self.cfg.retry, attempt)
                time.sleep(sleep_s)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("call_api failed without exception")


# Global singleton for convenience (local-only, in-process).
_DEFAULT = APIResilience()


def get_state(account_id: int) -> CircuitState:
    return _DEFAULT.circuit.get_state(int(account_id))


def is_open(account_id: int) -> bool:
    return _DEFAULT.circuit.is_open(int(account_id))


def cooldown_remaining_s(account_id: int) -> float:
    return _DEFAULT.circuit.cooldown_remaining_s(int(account_id))


def record_success(account_id: int) -> None:
    _DEFAULT.circuit.record_success(int(account_id))


def record_failure(account_id: int, error_type: str = "unknown") -> None:
    _DEFAULT.circuit.record_failure(int(account_id), error_type=error_type)


def call_api(
    account_id: int,
    fn: Callable[[], T],
    *,
    op_name: str,
    retry: bool = True,
    cost: float = 1.0,
    timeout_s: float = 1.0,
    on_failure: Optional[Callable[[BaseException, int], None]] = None,
    on_success: Optional[Callable[[int], None]] = None,
) -> T:
    return _DEFAULT.call_api(
        account_id,
        fn,
        op_name=op_name,
        retry=retry,
        cost=cost,
        timeout_s=timeout_s,
        on_failure=on_failure,
        on_success=on_success,
    )
