"""Dataiku date formats -> Spark datetime patterns (pieges_semantiques.md, "Patterns de date").

Dataiku formats follow Java ``SimpleDateFormat``; Spark 3+ patterns follow
``DateTimeFormatter``. Most letters agree, some do not (``Y`` week year, ``u`` day of
week, ``S`` fraction, names that depend on the language, zones). Only the letters of
``SAME_MEANING`` are converted, with the same count; quoted literals and separators
are kept. Anything else raises UnsupportedConstruct: a pattern is never guessed.
"""

from __future__ import annotations

from dku2dbx.errors import UnsupportedConstruct

# letter -> allowed repetition counts, same meaning in SimpleDateFormat and Spark.
SAME_MEANING: dict[str, frozenset[int]] = {
    "y": frozenset({2, 4}),  # year (yy: two digits)
    "M": frozenset({1, 2}),  # month number (MMM/MMMM are language-dependent names)
    "d": frozenset({1, 2}),  # day of month
    "H": frozenset({1, 2}),  # hour 0-23
    "m": frozenset({1, 2}),  # MINUTE (not month: the trap of the ROADMAP)
    "s": frozenset({1, 2}),  # second
    "S": frozenset({3}),  # milliseconds, exactly 3 digits (same value both sides)
}
SEPARATORS = frozenset(" -/:.,_")  # a literal T must be quoted, as in SimpleDateFormat


def spark_pattern(pattern: str, recipe: str, where: str) -> str:
    """The Spark pattern of a Dataiku date format, or UnsupportedConstruct."""
    out: list[str] = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == "'":
            end = pattern.find("'", i + 1)
            if end == -1:
                raise UnsupportedConstruct(recipe, where, f"unclosed quote in '{pattern}'")
            out.append(pattern[i : end + 1])
            i = end + 1
            continue
        if char.isalpha():
            rest = pattern[i:]
            run = len(rest) - len(rest.lstrip(char))
            if char not in SAME_MEANING or run not in SAME_MEANING[char]:
                raise UnsupportedConstruct(
                    recipe, where, f"'{char * run}' in '{pattern}' is not mapped (meaning differs)"
                )
            out.append(char * run)
            i += run
            continue
        if char not in SEPARATORS:
            raise UnsupportedConstruct(recipe, where, f"character '{char}' in '{pattern}'")
        out.append(char)
        i += 1
    return "".join(out)
