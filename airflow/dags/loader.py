"""Loader that turns the YAML files in this directory into Airflow DAGs.

Airflow only executes Python files, so the Blueprint YAML definitions need this
module to build and register them. The function name contains both "airflow" and
"dag", which is what Airflow's safe-mode file scanner looks for.

Blueprint classes live in ../blueprints, outside the dags folder that Blueprint
searches by default, so the registry is pointed at both that directory and this
one (which holds the project's DAG-args template).
"""

from pathlib import Path

from blueprint import BlueprintRegistry, build_all_airflow_dags

_DAGS_DIR = Path(__file__).resolve().parent
_BLUEPRINTS_DIR = _DAGS_DIR.parent / "blueprints"

build_all_airflow_dags(
    search_path=_DAGS_DIR,
    register_globals=globals(),
    bp_registry=BlueprintRegistry(template_dirs=[_BLUEPRINTS_DIR, _DAGS_DIR]),
)
