"""`python -m ftq ...` is the same as the `ftq` console script."""

from ftq.cli import app

# The guard matters: process-pool children are started with "spawn", which re-imports
# the parent's main module in each child. Without it, every child would start the CLI.
if __name__ == "__main__":
    app()
