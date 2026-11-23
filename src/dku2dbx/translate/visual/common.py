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


def columns_with_types(
    project: Project, dataset: str, recipe: Recipe
) -> list[tuple[str, str | None]]:
    schema(project, dataset, recipe)  # raises if unknown or empty
    return [(c.name, c.type) for c in project.dataset(dataset).columns]


def require_empty(recipe: Recipe, payload: dict[str, Any], *keys: str) -> None:
    """Options that change the result must be absent or empty until they are mapped."""
    for key in keys:
        if payload.get(key):
            raise UnsupportedConstruct(recipe.name, key, "option not mapped yet")


def require_false(recipe: Recipe, payload: dict[str, Any], *keys: str) -> None:
    for key in keys:
        if payload.get(key):
            raise UnsupportedConstruct(recipe.name, key, "option not mapped yet")


def check_output(recipe: Recipe, produced: list[str], declared: list[str], target: str) -> None:
    """The SQL must produce exactly the declared output schema, in order."""
    if produced != declared:
        raise UnsupportedConstruct(
            recipe.name,
            "output schema",
            f"'{target}' declares {declared}, the translation produces {produced}",
        )
