"""ftq: a fault-tolerant distributed task queue on Redis Streams.

Delivery is at-least-once; side effects are effectively-once via idempotency keys.
See docs/SPEC.md §4 for the full contract and docs/DECISIONS.md for why.
"""

__version__ = "0.1.0"
