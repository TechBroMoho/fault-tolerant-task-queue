"""`python -m ftq ...` is the same as the `ftq` console script."""

from ftq.cli import app

# Conventional hygiene, not a fix: multiprocessing's "spawn" does NOT re-run a package's
# __main__ module in pool children (CPython's spawn._fixup_main_from_name skips any
# `*.__main__`), so the process pool is safe either way (ADR-028).
if __name__ == "__main__":
    app()
