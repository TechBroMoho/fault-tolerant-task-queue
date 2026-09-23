"""Docs that must track the code: the README's configuration table (SPEC §8: every knob
documented), and the chaos transcript the README links (chaos/cast_text.py)."""

import json
import re
from pathlib import Path

from chaos.cast_text import cast_text
from ftq.config import Settings

README = Path(__file__).resolve().parents[2] / "README.md"


def test_the_readme_documents_every_setting_and_only_real_ones() -> None:
    rows = set(re.findall(r"^\| `(FTQ_[A-Z_]+)` \|", README.read_text(), re.MULTILINE))
    settings = {f"FTQ_{name.upper()}" for name in Settings.model_fields}
    assert rows == settings


def test_a_cast_becomes_its_plain_text() -> None:
    events = [[0.1, "o", "\x1b[32mok\x1b[0m\r\n"], [0.2, "i", "typed"], [0.3, "o", "50%\rdone\r\n"]]
    cast = "\n".join([json.dumps({"version": 2, "width": 80, "height": 24}),
                      *map(json.dumps, events)])  # fmt: skip
    assert cast_text(cast) == "ok\n50%done\n"  # input events and escapes dropped
