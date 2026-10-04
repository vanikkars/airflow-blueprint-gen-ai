"""Validate generated YAML before it is allowed near the dags folder.

A malformed DAG file does not fail alone - Airflow's loader imports every YAML
in the folder through one module, so a bad file can take the whole dagbag with
it. Everything here runs before the file is written, and the chain feeds any
failure back to the model for another attempt.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .introspect import BLUEPRINTS_DIR, DAGS_DIR

# Top-level keys ProjectDagArgsConfig declares in airflow/dags/dag_args.py.
# Anything else is rejected by Pydantic when Airflow parses the file.
ALLOWED_TOP_LEVEL = {
    "dag_id",
    "description",
    "schedule",
    "start_date",
    "catchup",
    "owner",
    "retries",
    "retry_delay_minutes",
    "steps",
}


@dataclass
class ValidationResult:
    """Outcome of validating one generated DAG."""

    ok: bool
    errors: list[str] = field(default_factory=list)

    def as_feedback(self) -> str:
        """Render the errors as a correction message for the model."""
        bullets = "\n".join(f"- {e}" for e in self.errors)
        return (
            "The YAML you produced failed validation against this repository. "
            f"Fix every problem below and return the corrected DAG.\n\n{bullets}"
        )


def validate_dag_yaml(text: str, dag_id: str, file_path: str) -> ValidationResult:
    """Check generated YAML for everything that would break at DAG-parse time."""
    errors: list[str] = []

    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return ValidationResult(False, [f"Not valid YAML: {exc}"])

    if not isinstance(doc, dict):
        return ValidationResult(False, ["Top level of the file must be a mapping."])

    # --- Top-level keys ---
    unknown = sorted(set(doc) - ALLOWED_TOP_LEVEL)
    if unknown:
        errors.append(
            f"Unknown top-level key(s) {unknown}. ProjectDagArgsConfig only accepts "
            f"{sorted(ALLOWED_TOP_LEVEL)}."
        )

    # --- dag_id, filename, uniqueness ---
    doc_dag_id = doc.get("dag_id")
    if not doc_dag_id:
        errors.append("Missing required key 'dag_id'.")
    else:
        if doc_dag_id != dag_id:
            errors.append(
                f"dag_id in the YAML ({doc_dag_id!r}) does not match the reported "
                f"dag_id ({dag_id!r})."
            )
        if not doc_dag_id.replace("_", "").isalnum() or doc_dag_id != doc_dag_id.lower():
            errors.append(f"dag_id {doc_dag_id!r} must be lowercase snake_case.")

        expected_path = f"airflow/dags/{doc_dag_id}.dag.yaml"
        if file_path != expected_path:
            errors.append(f"file_path must be {expected_path!r}, got {file_path!r}.")

        existing = {p.name.removesuffix(".dag.yaml") for p in DAGS_DIR.glob("*.dag.yaml")}
        if doc_dag_id in existing:
            errors.append(
                f"dag_id {doc_dag_id!r} already exists in airflow/dags/. Choose a "
                "different id or state that the user must confirm an overwrite."
            )

    if "start_date" in doc and not isinstance(doc["start_date"], str):
        errors.append(
            f"start_date must be a quoted ISO string, got {type(doc['start_date']).__name__}. "
            "An unquoted YAML date parses to a date object and fails validation."
        )

    # --- Steps, validated against the real blueprint config models ---
    steps = doc.get("steps")
    if not isinstance(steps, dict) or not steps:
        errors.append("'steps' must be a non-empty mapping of task-group id to step config.")
        return ValidationResult(not errors, errors)

    from blueprint import BlueprintRegistry

    registry = BlueprintRegistry(template_dirs=[BLUEPRINTS_DIR, DAGS_DIR])
    registry.discover()
    known = {bp["name"] for bp in registry.list_blueprints()}

    for step_id, step in steps.items():
        if not isinstance(step, dict):
            errors.append(f"Step {step_id!r} must be a mapping.")
            continue

        name = step.get("blueprint")
        if not name:
            errors.append(f"Step {step_id!r} is missing the 'blueprint' key.")
            continue
        if name not in known:
            errors.append(
                f"Step {step_id!r} references unknown blueprint {name!r}. "
                f"Available: {sorted(known)}."
            )
            continue

        config = {k: v for k, v in step.items() if k != "blueprint"}
        model = registry.get(name).get_config_type()

        # Unknown keys must be caught by hand. The blueprint config models do
        # not set extra="forbid", so Pydantic silently DROPS a key it does not
        # recognise. Left unchecked, an invented key such as
        # `incremental_column` would validate cleanly and the DAG would run a
        # full overwrite while appearing to do an incremental load.
        declared = set(model.model_fields)
        invented = sorted(set(config) - declared)
        if invented:
            errors.append(
                f"Step {step_id!r} sets key(s) {invented}, which blueprint {name!r} "
                f"does not define. These would be silently ignored at runtime, so the "
                f"DAG would not do what the keys imply. Allowed keys: {sorted(declared)}."
            )

        try:
            model.model_validate(config)
        except Exception as exc:
            errors.append(f"Step {step_id!r} failed {name} config validation: {exc}")

        # `mode` is typed as a bare str with no enum, so Pydantic accepts any
        # string. The blueprint only branches on 'overwrite' and 'append';
        # anything else falls through to the append path.
        mode = config.get("mode")
        if mode is not None and mode not in {"overwrite", "append"}:
            errors.append(
                f"Step {step_id!r} sets mode={mode!r}. The blueprint only implements "
                "'overwrite' and 'append'; any other value silently behaves as append."
            )

    return ValidationResult(not errors, errors)


def write_dag(text: str, file_path: str, repo_root: Path) -> Path:
    """Write validated YAML to its destination, refusing to clobber."""
    target = repo_root / file_path
    if target.exists():
        raise FileExistsError(f"{target} already exists; refusing to overwrite.")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text if text.endswith("\n") else text + "\n")
    return target
