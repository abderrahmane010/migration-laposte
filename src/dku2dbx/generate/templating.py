"""jinja2 templates of the generated bundle (``templates/`` at the repository root).

The templates ship in the wheel (pyproject, force-include); in a source checkout they
are read from the repository. Every Dataiku value goes through ``tojson``: JSON is
valid YAML, so no name can break the generated file.
"""

from __future__ import annotations

import json
from importlib import resources
from pathlib import Path
from typing import Any

import jinja2

_REPO_TEMPLATES = Path(__file__).resolve().parents[3] / "templates"


def _loader() -> jinja2.BaseLoader:
    packaged = resources.files("dku2dbx").joinpath("templates")
    if packaged.is_dir():
        return jinja2.FileSystemLoader(str(packaged))
    return jinja2.FileSystemLoader(str(_REPO_TEMPLATES))


_ENV = jinja2.Environment(
    loader=_loader(),
    autoescape=False,
    keep_trailing_newline=True,
    undefined=jinja2.StrictUndefined,
)
_ENV.filters["tojson"] = lambda value: json.dumps(value, ensure_ascii=False)


def render(template: str, **context: Any) -> str:
    return _ENV.get_template(template).render(**context)
