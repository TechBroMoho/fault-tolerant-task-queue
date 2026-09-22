"""Retry delays: exponential backoff with full jitter (ADR-026).

    delay = random(0, min(cap, base * 2**attempt))

Why jitter: when many jobs fail together (the email API goes down for a minute), plain
exponential backoff retries them all at the same instants, and each retry wave hits the
recovering service at once. Full jitter spreads each wave evenly over its window, which
AWS's analysis found gives the least total work and the fastest recovery of the common
variants (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
"""

import random

# 2**62 seconds is far beyond any sane cap; clamping the exponent keeps the float
# arithmetic finite however large `attempt` gets.
_MAX_EXPONENT = 62


def backoff_bound(attempt: int, base: float, cap: float) -> float:
    """Upper bound (s) of the delay after failed attempt number `attempt` (0-based)."""
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    return min(cap, base * 2.0 ** min(attempt, _MAX_EXPONENT))


def full_jitter_delay(attempt: int, base: float, cap: float, rng: random.Random) -> float:
    """Delay (s) before retrying after failed attempt `attempt`: uniform in [0, bound]."""
    return rng.uniform(0, backoff_bound(attempt, base, cap))
