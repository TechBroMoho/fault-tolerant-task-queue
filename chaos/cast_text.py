"""The plain text of an asciicast v2 recording (`make chaos-record`).

    uv run python -m chaos.cast_text results/local/chaos_demo.cast > chaos_demo.txt

GitHub can't play a `.cast`, so the README links this transcript beside it: the same
output, without the timing. Replay the real thing with
`uvx --from asciinema==2.4.0 asciinema play results/local/chaos_demo.cast`.
"""

import json
import re
import sys
from pathlib import Path

# CSI sequences (colors, cursor moves) and OSC sequences (window titles).
_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07]*\x07")


def cast_text(cast: str) -> str:
    """Every output event's text in order, with ANSI escapes and carriage returns removed."""
    lines = cast.splitlines()
    json.loads(lines[0])  # the header: fail loudly on something that isn't a cast
    out = "".join(e[2] for e in map(json.loads, lines[1:]) if e[1] == "o")
    return _ANSI.sub("", out).replace("\r\n", "\n").replace("\r", "")


def main() -> None:
    sys.stdout.write(cast_text(Path(sys.argv[1]).read_text()))


if __name__ == "__main__":
    main()
