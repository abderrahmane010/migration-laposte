"""dku2dbx — Databricks App (ROADMAP phase 10): the migration journey in a browser.

Déposer un export ZIP → Analyser → Simplifier → Brancher → Migrer → Réconcilier.
Every step calls dku2dbx.webapp.session, i.e. the code of the CLI; this file only
lays out the pages. Local run: ``uv run streamlit run app/app.py``.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from dku2dbx import __version__
from dku2dbx.errors import Dku2dbxError
from dku2dbx.webapp import session as journey

STEPS = ["1. Déposer", "2. Analyser", "3. Simplifier", "4. Brancher", "5. Migrer", "6. Réconcilier"]

st.set_page_config(page_title="dku2dbx — Dataiku → Databricks", page_icon="🔁", layout="wide")
state = st.session_state


def _workdir() -> Path:
    if "workdir" not in state:
        state.workdir = tempfile.mkdtemp(prefix="dku2dbx-")
    return Path(state.workdir)


def _require_session() -> journey.Session | None:
    if "session" not in state:
        st.info("Déposez d'abord un export ZIP (étape 1).")
        return None
    current: journey.Session = state.session
    return current


def page_upload() -> None:
    st.header("1. Déposer un export Dataiku")
    st.write(
        "Export ZIP d'un projet Dataiku, **configuration seulement** : dans Dataiku, "
        "*Actions* → *Export this project*, en décochant les données et les fichiers "
        "chargés. Le fichier n'est jamais exécuté."
    )
    upload = st.file_uploader("Export ZIP", type=["zip"])
    if upload is None:
        return
    data = upload.getvalue()
    if state.get("upload_name") == upload.name and "session" in state:
        st.success(f"Projet **{state.session.project.key}** chargé.")
        return
    try:
        state.session = journey.open_export(data, upload.name, _workdir() / "projet")
    except Dku2dbxError as exc:
        st.error(f"Export illisible : {exc}")
        return
    state.upload_name = upload.name
    for stale in ("migration", "reconciliation", "branching_md"):
        state.pop(stale, None)
    stored = journey.store_export(data, upload.name)
    project = state.session.project
    st.success(
        f"Projet **{project.key}** : {len(project.datasets)} datasets, "
        f"{len(project.recipes)} recettes, {len(project.scenarios)} scénario(s)."
    )
    if stored:
        st.caption(f"Export copié dans le Volume : `{stored}`")


def page_analyze() -> None:
    st.header("2. Analyser")
    current = _require_session()
    if current is None:
        return
    result, report = journey.analysis(current)
    cols = st.columns(4)
    cols[0].metric("Recettes", sum(result.recipes_by_class.values()))
    cols[1].metric("Niveau", result.level.value)
    cols[2].metric("Effort estimé", f"{result.effort_days:g} j")
    cols[3].metric("Points à examiner", len(current.project.issues))
    problems = journey.issues(current)
    if problems:
        st.subheader("Points à examiner")
        st.dataframe(pd.DataFrame(problems), hide_index=True, width="stretch")
    else:
        st.success("Export complet : aucun point à examiner.")
    st.subheader("Recettes par classe cible")
    st.bar_chart(pd.Series(result.recipes_by_class, name="recettes"))
    with st.expander("Rapport complet (analyse.md)"):
        st.markdown(report)
    st.download_button("Télécharger analyse.md", report, file_name="analyse.md")


def page_simplify() -> None:
    st.header("3. Simplifier")
    current = _require_session()
    if current is None:
        return
    flow, report = journey.simplification(current)
    cols = st.columns(3)
    cols[0].metric("Recettes", flow.recipes_before)
    cols[1].metric("Étapes", len(flow.steps))
    cols[2].metric("Tables intermédiaires supprimées", len(flow.removed))
    st.markdown(report)


def page_connect() -> None:
    st.header("4. Brancher les sources")
    current = _require_session()
    if current is None:
        return
    groups = journey.sources_by_connection(current)
    if not groups:
        st.success("Aucune source à brancher : le projet ne lit aucune table externe.")
        return
    st.write(
        "Une ligne par connexion Dataiku : le catalogue et le schéma Unity Catalog où "
        "se trouvent ses tables. `{table}` est le nom de la table Dataiku (ou du dataset), "
        "`{dataset}` le nom du dataset. Une ligne sans catalogue reste « à brancher »."
    )
    rows = pd.DataFrame(
        [
            {
                "connection": name,
                "sources": ", ".join(datasets),
                "catalog": "",
                "schema": "",
                "table": "{table}",
                "lowercase": False,
            }
            for name, datasets in groups.items()
        ]
    )
    edited = st.data_editor(
        rows,
        disabled=["connection", "sources"],
        hide_index=True,
        width="stretch",
        key="connections_editor",
    )
    if st.button("Brancher", type="primary"):
        try:
            branching, report = journey.connect(current, edited.to_dict("records"))
        except Dku2dbxError as exc:
            st.error(str(exc))
            return
        state.branching_md = report
        state.pop("migration", None)
        st.success(
            f"{len(branching.resolved)} source(s) branchée(s), "
            f"{len(branching.unresolved)} à brancher."
        )
    if "branching_md" in state:
        st.markdown(state.branching_md)


def page_migrate() -> None:
    st.header("5. Migrer")
    current = _require_session()
    if current is None:
        return
    if current.branching is None:
        st.warning("Les sources ne sont pas branchées (étape 4) : leurs tâches échoueront.")
    target = st.radio(
        "Cible",
        ["job", "pipeline"],
        horizontal=True,
        help="job : notebooks SQL ; "
        "pipeline : Lakeflow pipeline de vues matérialisées, plus un job.",
    )
    llm = journey.llm_config()
    st.caption(
        f"LLM : endpoint `{llm.endpoint}`"
        if llm.enabled
        else "LLM désactivé : le résidu est seulement préparé (aucun endpoint configuré)."
    )
    if st.button("Générer le bundle", type="primary"):
        try:
            state.migration = journey.migrate(current, target=target)
        except Dku2dbxError as exc:
            st.error(str(exc))
            return
    migration: journey.Migration | None = state.get("migration")
    if migration is None:
        return
    manual = [g for g in migration.bundle.steps if not g.translated]
    cols = st.columns(3)
    cols[0].metric("Étapes", len(migration.bundle.steps))
    cols[1].metric("À migrer à la main", len(manual))
    cols[2].metric("Résidu LLM", len(migration.residue.items))
    st.download_button(
        "Télécharger le bundle (ZIP)",
        migration.archive,
        file_name=f"{current.project.key}_bundle.zip",
        mime="application/zip",
        type="primary",
    )
    st.caption(
        "Déploiement après relecture : `databricks bundle validate`, puis "
        "`databricks bundle deploy` dans le dossier `bundle/`."
    )
    with st.expander("Bilan de génération (GENERATION.md)", expanded=True):
        st.markdown(migration.report)
    with st.expander("Résidu LLM (RESIDU.md)"):
        st.markdown(migration.residue_report)


def page_reconcile() -> None:
    st.header("6. Réconcilier")
    current = _require_session()
    if current is None:
        return
    st.write(
        "Clé métier et tolérances par table. Sans clé, la comparaison ligne à ligne se "
        "fait par `EXCEPT ALL` (aucune tolérance possible)."
    )
    rows = pd.DataFrame(
        [
            {
                "table": name,
                "key": "",
                "abs_tolerance": 0.0,
                "rel_tolerance": 0.0,
                "ignore_columns": "",
                "skip": False,
            }
            for name in journey.reconcile_tables(current)
        ]
    )
    edited = st.data_editor(
        rows, disabled=["table"], hide_index=True, width="stretch", key="reconcile_editor"
    )
    if st.button("Générer la réconciliation", type="primary"):
        try:
            state.reconciliation = journey.reconcile(current, edited.to_dict("records"))
        except (Dku2dbxError, ValueError) as exc:
            st.error(str(exc))
            return
    files: journey.ReconciliationFiles | None = state.get("reconciliation")
    if files is None:
        return
    st.download_button(
        "Télécharger le notebook, le plan et le registre (ZIP)",
        files.archive,
        file_name=f"{current.project.key}_reconciliation.zip",
        mime="application/zip",
        type="primary",
    )
    for warning in files.plan.warnings:
        st.warning(warning)
    st.markdown(files.plan_report)


PAGES = {
    STEPS[0]: page_upload,
    STEPS[1]: page_analyze,
    STEPS[2]: page_simplify,
    STEPS[3]: page_connect,
    STEPS[4]: page_migrate,
    STEPS[5]: page_reconcile,
}

with st.sidebar:
    st.title("dku2dbx")
    st.caption(f"Migration Dataiku → Databricks — v{__version__}")
    choice = st.radio("Étape", STEPS, key="step")
    if "session" in state:
        st.divider()
        st.write(f"Projet : **{state.session.project.key}**")

PAGES[choice]()
