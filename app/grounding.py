"""Numeric grounding check for AI output.

Every number in an AI answer must already appear in the facts the AI was given
(the question and the tool results). Anything else is flagged as unverified,
and an ungrounded answer cannot be approved unchanged.
"""
from __future__ import annotations

import json
import re
from decimal import Decimal

_NUM = re.compile(r"(?<![A-Za-z_\-])-?\d+(?:[.,]\d+)*")


def numbers(text: str) -> set[str]:
    out = set()
    for tok in _NUM.findall(text or ""):
        t = tok.replace(",", "").lstrip("-")
        try:
            out.add(str(Decimal(t).normalize()))
        except Exception:
            pass
    return out


def is_grounded(text: str, facts) -> tuple[bool, list[str]]:
    allowed = numbers(json.dumps(facts, default=str))
    # Single digits are list markers and step numbers far more often than facts.
    found = {n for n in numbers(text) if not (len(n) == 1 and n.isdigit())}
    unknown = sorted(found - allowed)
    return (not unknown, unknown)
