"""Per-answer validation for the guided wizard.

Each function here validates one slot against something real - the live source
catalog, the registered blueprints, the Airflow connection list, the dags folder
- and returns an `Answer` rather than raising. The wizard decides what to do
with it, which keeps the policy in one place:

    hard error  -> re-ask the question (the answer cannot be used)
    soft warning -> accept the answer, carry the warning to the summary

The split matters because the two failure kinds have different causes. A hard
error means the user named something that does not exist, so no YAML could be
correct. A soft warning means the request is buildable but has a consequence
worth seeing before it runs - PII landing unmasked, money becoming floating
point, `overwrite` discarding snapshot history. Blocking on those would stop
legitimate pipelines; hiding them would ship a surprise.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import capabilities
from .introspect import (
    DAGS_DIR,
    LOSSY_PG_TYPES,
    MAPPED_PG_TYPES,
    SENSITIVE_PATTERNS,
)

# Airflow's own schedule presets, plus the two ways to say "never".
SCHEDULE_PRESETS = {
    "@once",
    "@hourly",
    "@daily",
    "@weekly",
    "@monthly",
    "@yearly",
    "@annually",
    "@continuous",
}
NO_SCHEDULE = {"none", "null", "manual", "never"}

# Airflow only implements these two write modes; see validate.py for why any
# other value is dangerous rather than merely wrong.
WRITE_MODES = ("overwrite", "append")

# A row estimate above this means the extract task's single in-memory DataFrame
# becomes the real constraint, so the wizard says so.
LARGE_TABLE_ROWS = 1_000_000

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass
class Answer:
    """One validated slot value.

    `ok` False means the wizard must ask again; `value` is then meaningless.
    `warnings` are carried forward on an accepted answer.
    """

    ok: bool
    value: object = None
    error: str = ""
    hint: str = ""
    warnings: list[str] = field(default_factory=list)

    @classmethod
    def accept(cls, value: object, warnings: list[str] | None = None) -> Answer:
        return cls(ok=True, value=value, warnings=warnings or [])

    @classmethod
    def reject(cls, error: str, hint: str = "") -> Answer:
        return cls(ok=False, error=error, hint=hint)


def _did_you_mean(word: str, options: list[str], limit: int = 3) -> list[str]:
    """Closest options to a mistyped name.

    Substring matches come first - a user typing `trans` for `transactions`
    means it - then difflib for genuine typos.
    """
    import difflib

    lowered = word.lower()
    substring = [o for o in options if lowered in o.lower() or o.lower() in lowered]
    fuzzy = difflib.get_close_matches(lowered, options, n=limit, cutoff=0.6)

    ordered: list[str] = []
    for candidate in substring + fuzzy:
        if candidate not in ordered:
            ordered.append(candidate)
    return ordered[:limit]


# --------------------------------------------------------------------------
# Integration: the first thing to validate, because nothing else matters if
# the source->target pair has no blueprint behind it.
# --------------------------------------------------------------------------


def validate_source_system(raw: str) -> Answer:
    """Check the source system is one a blueprint can read from."""
    system = capabilities.normalize(raw)
    if not system:
        return Answer.reject("Please name a source system.")

    sources = capabilities.known_sources()
    if system in sources:
        return Answer.accept(system)

    # The user may have named a system this repo can only *write* to.
    if system in capabilities.known_targets():
        return Answer.reject(
            f"UNSUPPORTED INTEGRATION: nothing can read from {system!r} here.",
            f"{system} is available as a destination, not as a source. "
            f"Supported sources: {', '.join(sources)}.",
        )

    suggestions = _did_you_mean(system, sources)
    hint = f"Supported sources: {', '.join(sources)}."
    if suggestions:
        hint = f"Did you mean {', '.join(suggestions)}? " + hint
    return Answer.reject(
        f"UNSUPPORTED INTEGRATION: no blueprint reads from {system!r}.", hint
    )


def validate_target_system(raw: str, source: str) -> Answer:
    """Check the target is reachable *from this source*.

    Validating the pair rather than the target alone is the point: a target
    could be supported in general and still be unreachable from the chosen
    source, and that is exactly the case worth warning about.
    """
    system = capabilities.normalize(raw)
    if not system:
        return Answer.reject("Please name a destination system.")

    integration = capabilities.find_integration(source, system)
    if integration is not None:
        return Answer.accept(system)

    reachable = capabilities.targets_for(source)
    lines = [
        f"{source} -> {system} is not a supported integration.",
        "",
        "This blueprint library can build:",
        capabilities.describe_support(),
    ]
    if reachable:
        lines.insert(
            1, f"From {source}, only these destinations exist: {', '.join(reachable)}."
        )

    return Answer.reject(
        f"UNSUPPORTED INTEGRATION: {source} -> {system}", "\n".join(lines)
    )


# --------------------------------------------------------------------------
# Source objects, validated against live introspection
# --------------------------------------------------------------------------


def validate_connection(raw: str, connections: dict[str, dict]) -> Answer:
    """Check the Airflow connection id exists.

    An unknown connection id fails only when the DAG *runs*, long after it was
    generated, so this is a hard error.
    """
    conn_id = raw.strip()
    if not conn_id:
        return Answer.reject("Please name an Airflow connection.")
    if conn_id in connections:
        return Answer.accept(conn_id)

    known = sorted(connections)
    suggestions = _did_you_mean(conn_id, known)
    hint = f"Known connections: {', '.join(known) if known else '(none found)'}."
    if suggestions:
        hint = f"Did you mean {', '.join(suggestions)}? " + hint
    return Answer.reject(f"No Airflow connection {conn_id!r} exists.", hint)


def validate_table(raw: str, catalog: dict[tuple[str, str], dict]) -> Answer:
    """Resolve a user's table reference against the real catalog.

    Accepts `transactions`, `public.transactions`, and `the transactions table`,
    because those are the three ways people actually type it. Resolving to a
    `(schema, table)` pair also catches the ambiguous case where the same table
    name exists in two schemas, which has to be a question rather than a guess.
    """
    text = raw.strip().strip('"').strip("'")
    text = re.sub(r"^(the)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+table$", "", text, flags=re.IGNORECASE).strip()

    if not text:
        return Answer.reject("Please name a source table.")

    qualified = "." in text
    if qualified:
        schema, _, table = text.rpartition(".")
        schema, table = schema.strip().lower(), table.strip().lower()
        matches = [key for key in catalog if key == (schema, table)]
    else:
        table = text.lower()
        matches = [key for key in catalog if key[1] == table]

    if len(matches) == 1:
        schema_name, table_name = matches[0]
        return Answer.accept(matches[0], _table_warnings(catalog[matches[0]], schema_name, table_name))

    if len(matches) > 1:
        options = ", ".join(f"{s}.{t}" for s, t in sorted(matches))
        return Answer.reject(
            f"{table!r} exists in more than one schema.",
            f"Qualify it: {options}.",
        )

    everything = sorted(f"{s}.{t}" for s, t in catalog)
    bare_names = sorted({t for _, t in catalog})
    suggestions = _did_you_mean(table, bare_names)
    hint = f"Tables in the source database: {', '.join(everything) if everything else '(none)'}."
    if suggestions:
        hint = f"Did you mean {', '.join(suggestions)}? " + hint
    return Answer.reject(f"No table {text!r} in the source database.", hint)


def _table_warnings(table: dict, schema_name: str, table_name: str) -> list[str]:
    """Soft findings about a table the blueprint will carry across as-is.

    Derived from the same catalog metadata the prompt is grounded on, so the
    wizard raises them at the moment the table is chosen rather than leaving
    them to the model.
    """
    warnings: list[str] = []
    unmapped: list[str] = []
    lossy: list[str] = []
    sensitive: list[str] = []

    for column in table["columns"]:
        name, data_type = column["name"], column["data_type"]
        if data_type not in MAPPED_PG_TYPES:
            unmapped.append(f"{name} ({data_type})")
        elif data_type in LOSSY_PG_TYPES:
            lossy.append(name)
        if any(pattern in name.lower() for pattern in SENSITIVE_PATTERNS):
            sensitive.append(name)

    if unmapped:
        warnings.append(
            f"Unmapped Postgres type(s) land in Iceberg as StringType: {', '.join(unmapped)}."
        )
    if lossy:
        warnings.append(
            f"numeric/decimal map to Iceberg DoubleType, so exact values become "
            f"floating point: {', '.join(lossy)}."
        )
    if sensitive:
        warnings.append(
            f"Possible PII with no masking in the blueprint - these land in "
            f"Iceberg in the clear: {', '.join(sensitive)}."
        )
    if not table["primary_key"]:
        warnings.append(
            f"{schema_name}.{table_name} has no primary key, so re-running cannot "
            "deduplicate rows."
        )

    rows = table["approx_rows"]
    if rows is not None and rows >= LARGE_TABLE_ROWS:
        warnings.append(
            f"{schema_name}.{table_name} holds ~{rows:,} rows. The extract task "
            "builds one in-memory DataFrame before writing Parquet - batch_size "
            "chunks the read only, so watch memory."
        )
    return warnings


# --------------------------------------------------------------------------
# Destination and DAG parameters
# --------------------------------------------------------------------------


def validate_identifier(raw: str, label: str) -> Answer:
    """Check a namespace or table name is a usable lowercase identifier."""
    value = raw.strip()
    if not value:
        return Answer.reject(f"Please give a {label}.")
    if not _IDENTIFIER.match(value):
        return Answer.reject(
            f"{value!r} is not a valid {label}.",
            "Use lowercase letters, digits and underscores, starting with a letter.",
        )
    return Answer.accept(value)


def validate_mode(raw: str) -> Answer:
    """Check the write mode is one the blueprint actually implements.

    `mode` is a bare `str` in the config model with no enum, so Pydantic accepts
    `merge` or `upsert` and the blueprint then silently takes the append path.
    Rejecting here is the only place that stops it.
    """
    mode = raw.strip().lower()
    if not mode:
        return Answer.reject("Please choose a write mode.")
    if mode not in WRITE_MODES:
        return Answer.reject(
            f"Write mode {mode!r} is not implemented.",
            f"Only {' and '.join(repr(m) for m in WRITE_MODES)} exist. The config "
            "model would accept any string and then quietly behave as 'append', "
            "so it cannot be used.",
        )

    warnings: list[str] = []
    if mode == "overwrite":
        warnings.append(
            "mode 'overwrite' drops and recreates the Iceberg table on every run, "
            "discarding snapshot history and time travel."
        )
    return Answer.accept(mode, warnings)


def validate_schedule(raw: str) -> Answer:
    """Check the schedule is a preset, a 5-field cron expression, or none."""
    text = raw.strip().strip('"').strip("'")
    if not text:
        return Answer.reject("Please give a schedule.")

    lowered = text.lower()
    if lowered in NO_SCHEDULE:
        return Answer.accept(
            None, ["Schedule is null, so this DAG only runs when triggered manually."]
        )
    if lowered in SCHEDULE_PRESETS:
        return Answer.accept(lowered)
    if lowered.startswith("@"):
        return Answer.reject(
            f"{text!r} is not an Airflow preset.",
            f"Presets: {', '.join(sorted(SCHEDULE_PRESETS))}.",
        )

    fields = text.split()
    if len(fields) != 5:
        return Answer.reject(
            f"{text!r} is not a valid schedule.",
            "Use a preset (@daily, @hourly), a 5-field cron expression "
            "(\"0 6 * * *\"), or 'none' for manual runs only.",
        )

    bounds = [(0, 59, "minute"), (0, 23, "hour"), (1, 31, "day of month"),
              (1, 12, "month"), (0, 7, "day of week")]
    for value, (low, high, label) in zip(fields, bounds):
        if not _cron_field_ok(value, low, high):
            return Answer.reject(
                f"Cron field {value!r} is not a valid {label}.",
                f"Expected {low}-{high}, a list, a range, or a step.",
            )
    return Answer.accept(text)


def _cron_field_ok(value: str, low: int, high: int) -> bool:
    """Check one cron field, allowing `*`, lists, ranges and steps."""
    for part in value.split(","):
        if not part:
            return False
        step = "1"
        if "/" in part:
            part, _, step = part.partition("/")
            if not step.isdigit() or int(step) == 0:
                return False
        if part == "*":
            continue
        if "-" in part.lstrip("-"):
            start, _, end = part.partition("-")
            if not (start.isdigit() and end.isdigit()):
                return False
            if not (low <= int(start) <= high and low <= int(end) <= high):
                return False
            if int(start) > int(end):
                return False
            continue
        if not part.isdigit() or not (low <= int(part) <= high):
            return False
    return True


def validate_start_date(raw: str) -> Answer:
    """Check the start date is an ISO date.

    Returned as a string, because an unquoted YAML date becomes a `date` object
    and fails `ProjectDagArgsConfig` validation at parse time.
    """
    from datetime import date, datetime

    text = raw.strip().strip('"').strip("'")
    if not text:
        return Answer.reject("Please give a start date.")
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return Answer.reject(
            f"{text!r} is not an ISO date.", "Use YYYY-MM-DD, e.g. 2024-01-01."
        )

    # Re-render from the parsed date rather than echoing the input: strptime
    # accepts unpadded `2024-1-1`, and that spelling would reach the YAML as-is.
    canonical = parsed.isoformat()

    warnings: list[str] = []
    if parsed > date.today():
        warnings.append(
            f"start_date {canonical} is in the future, so no run is scheduled until then."
        )
    return Answer.accept(canonical, warnings)


def validate_dag_id(raw: str) -> Answer:
    """Check the dag_id is snake_case and not already taken.

    A colliding dag_id is a hard error: `write_dag` refuses to clobber the file,
    and two DAGs sharing an id confuses Airflow itself.
    """
    dag_id = raw.strip()
    if not dag_id:
        return Answer.reject("Please give a dag_id.")
    if not _IDENTIFIER.match(dag_id):
        return Answer.reject(
            f"dag_id {dag_id!r} is not valid.",
            "Use lowercase snake_case: letters, digits and underscores, starting "
            "with a letter. No dots, spaces or capitals.",
        )

    existing = {path.name.removesuffix(".dag.yaml") for path in DAGS_DIR.glob("*.dag.yaml")}
    if dag_id in existing:
        return Answer.reject(
            f"dag_id {dag_id!r} already exists in airflow/dags/.",
            f"Pick another. Taken: {', '.join(sorted(existing))}.",
        )
    return Answer.accept(dag_id)


def validate_batch_size(raw: str, default: int) -> Answer:
    """Check batch_size is a positive integer."""
    text = raw.strip()
    if not text:
        return Answer.accept(default)
    if not text.isdigit() or int(text) == 0:
        return Answer.reject(
            f"{text!r} is not a positive whole number of rows.",
            "batch_size is the Postgres read chunk, e.g. 10000.",
        )

    size = int(text)
    warnings: list[str] = []
    if size > 1_000_000:
        warnings.append(
            f"batch_size {size:,} is very large; each chunk is held in memory "
            "before the chunks are concatenated."
        )
    return Answer.accept(size, warnings)
