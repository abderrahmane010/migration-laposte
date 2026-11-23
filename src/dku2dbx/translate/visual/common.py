"""Shared helpers of the visual recipe translators."""

from __future__ import annotations

from typing import Any

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Project, Recipe, RecipeInput, RecipeOutput


def main_inputs(recipe: Recipe) -> list[RecipeInput]:
    return [i for i in recipe.inputs if i.role == "main"]


def main_outputs(recipe: Recipe) -> list[RecipeOutput]:
    return [o for o in recipe.outputs if o.role == "main"]


def single_input(recipe: Recipe) -> RecipeInput:
    inputs = main_inputs(recipe)
    if len(inputs) != 1 or len(recipe.inputs) != 1:
        raise UnsupportedConstruct(
            recipe.name, "inputs", f"exactly one input expected, got {len(recipe.inputs)}"
        )
    return inputs[0]


def single_output(recipe: Recipe) -> RecipeOutput:
    outputs = main_outputs(recipe)
    if len(outputs) != 1 or len(recipe.outputs) != 1:
        raise UnsupportedConstruct(
            recipe.name, "outputs", f"exactly one output expected, got {len(recipe.outputs)}"
        )
    return outputs[0]


def schema(project: Project, dataset: str, recipe: Recipe) -> list[str]:
    """Column names of a dataset of the project (a translation needs them)."""
    try:
        columns = [c.name for c in project.dataset(dataset).columns]
    except KeyError:
        raise UnsupportedConstruct(
            recipe.name, "dataset", f"'{dataset}' is not a dataset of the project (shared?)"
        ) from None
    if not columns:
        raise UnsupportedConstruct(recipe.name, "schema", f"dataset '{dataset}' has no schema")
    return columns


def payload_dict(recipe: Recipe) -> dict[str, Any]:
    if not isinstance(recipe.payload, dict):
        raise UnsupportedConstruct(recipe.name, "payload", "missing or not a JSON object")
    return recipe.payload


def partition_warnings(recipe: Recipe) -> list[str]:
    """Partition dependencies are applied by the job (phase 6): say so, never drop them."""
    return [
        f"dépendance de partition de l'entrée '{i.ref}' non traduite ici (phase 6)"
        for i in recipe.inputs
        if i.deps
    ]


def require_no_filter(recipe: Recipe, flt: Any, where: str) -> None:
    """Accept a disabled filter only.

    ASSUMPTION (H-09h): {enabled, distinct, uiData}. The operator vocabulary of
    an enabled filter is not known yet: never guess it.
    """
    if flt is None:
        return
    if not isinstance(flt, dict):
        raise UnsupportedConstruct(recipe.name, where, "filter is not a JSON object")
    if flt.get("enabled"):
        raise UnsupportedConstruct(
            recipe.name, where, "enabled filter (operator vocabulary not mapped yet)"
        )
