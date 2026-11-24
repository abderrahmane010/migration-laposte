"""Registry of the deterministic recipe translators (phases 3 and 4)."""

from __future__ import annotations

from collections.abc import Callable

from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.code.sql_recipe import translate_sql_query
from dku2dbx.translate.model import Translation
from dku2dbx.translate.prepare.recipe import translate_prepare
from dku2dbx.translate.visual.group import translate_grouping
from dku2dbx.translate.visual.join import translate_join
from dku2dbx.translate.visual.pivot import translate_pivot
from dku2dbx.translate.visual.simple import translate_distinct, translate_sort, translate_vstack
from dku2dbx.translate.visual.split import translate_split
from dku2dbx.translate.visual.sync import translate_sampling, translate_sync
from dku2dbx.translate.visual.topn import translate_topn
from dku2dbx.translate.visual.window import translate_window

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
    "window": translate_window,
    "split": translate_split,
    "topn": translate_topn,
    "pivot": translate_pivot,
    # Phase 4: Prepare recipes (processor registry in translate/prepare).
    "shaker": translate_prepare,
    # Phase 8: SQL code recipes, transpiled deterministically, always to review.
    "sql_query": translate_sql_query,
}
