"""Deterministic translators of the visual recipes (ROADMAP phase 3)."""

from __future__ import annotations

from collections.abc import Callable

from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.model import Translation
from dku2dbx.translate.visual.group import translate_grouping
from dku2dbx.translate.visual.join import translate_join
from dku2dbx.translate.visual.simple import translate_distinct, translate_sort, translate_vstack
from dku2dbx.translate.visual.sync import translate_sampling, translate_sync

Translator = Callable[[Project, Recipe], Translation]

# Recipe type -> translator. Phase 3 adds them in the ROADMAP order; the phase 2
# classification reads this registry ("translator available").
TRANSLATORS: dict[str, Translator] = {
    "sync": translate_sync,
    "sampling": translate_sampling,
    "distinct": translate_distinct,
    "vstack": translate_vstack,
    "sort": translate_sort,
    "join": translate_join,
    "grouping": translate_grouping,
}
