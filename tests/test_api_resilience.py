from __future__ import annotations

import time

from project_dragon.api_resilience import (
    APIResilience,
    CircuitBreakerConfig,
    CircuitState,
    RateLimitConfig,
    ResilienceConfig,
    RetryConfig,
)


def test_circuit_breaker_opens_and_recovers() -> None:
    cfg = ResilienceConfig(
        rate_limit=RateLimitConfig(rate_per_s=1_000.0, burst=1_000.0),
        retry=RetryConfig(max_attempts=1, base_backoff_s=0.0, max_backoff_s=0.0, jitter_frac=0.0),
        circuit=CircuitBreakerConfig(failure_window_s=60.0, open_after_failures=3, cooldown_s=0.2),
    )
    r = APIResilience(cfg)
    acct = 42

    # Trip the breaker
    for _ in range(3):
        try:
            r.call_api(acct, lambda: (_ for _ in ()).throw(TimeoutError("net")), op_name="x", retry=False)
        except TimeoutError:
            pass

    assert r.circuit.get_state(acct) == CircuitState.OPEN

    # Cooldown -> HALF_OPEN
    time.sleep(0.25)
    assert r.circuit.get_state(acct) in (CircuitState.HALF_OPEN, CircuitState.CLOSED)

    # A successful probe should close
    r.call_api(acct, lambda: 123, op_name="probe", retry=False)
    assert r.circuit.get_state(acct) == CircuitState.CLOSED
