"""Guided interview that builds one pipeline YAML by asking for each parameter.

The free-form chat in `chat.py` takes a whole request at once and leans on the
model to notice what is missing. This module inverts that: it owns the
conversation, asks for one parameter at a time, and validates every answer
locally before moving on.

That ordering is the design. The integration question comes first, so an
unsupported source->target pair is refused against the live blueprint registry
before a single token is spent, and the table question is checked against real
introspection rather than the model's recollection of it. By the time the YAML
is produced, every value in it is already known to be real.

    integration -> connection -> table -> destination -> load -> schedule
         |              |           |
         +-- registry   +-- conn    +-- live catalog
             (hard)         list        (hard: unknown table)
                            (hard)      (soft: PII, precision, volume)

Hard failures re-ask. Soft findings are collected and shown at the summary,
which is also where the only confirmation lives - nothing is written until the
user accepts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

import yaml

from . import capabilities, slots
from .introspect import DAGS_DIR, REPO_ROOT, Grounding, SourceConnection, gather_grounding
from .validate import validate_dag_yaml, write_dag

logger = logging.getLogger(__name__)

GREEN, YELLOW, RED, BLUE, CYAN, DIM, BOLD, RESET = (
    "\033[32m", "\033[33m", "\033[31m", "\033[34m", "\033[36m",
    "\033[2m", "\033[1m", "\033[0m",
)

TICK, CROSS, WARN, ASK = "✓", "✗", "⚠", "?"

# Sentinel raised by the prompt helpers when the user abandons the interview.
class Cancelled(Exception):
    """The user typed `cancel`, or sent EOF/interrupt."""


@dataclass
class Spec:
    """The pipeline being specified, filled in one slot at a time."""

    source_system: str = ""
    target_system: str = ""
    blueprint: str = ""
    conn_id: str = ""
    schema_name: str = ""
    table_name: str = ""
    iceberg_namespace: str = ""
    iceberg_table_name: str = ""
    s3_bucket: str = "iceberg-warehouse"
    s3_prefix: str = "warehouse"
    mode: str = "overwrite"
    batch_size: int = 10000
    schedule: str | None = "@daily"
    start_date: str = "2024-01-01"
    dag_id: str = ""
    description: str = ""
    warnings: list[str] = field(default_factory=list)

    def note(self, found: list[str]) -> None:
        """Collect soft findings, keeping them unique and in order."""
        for warning in found:
            if warning not in self.warnings:
                self.warnings.append(warning)


# --------------------------------------------------------------------------
# Terminal plumbing
# --------------------------------------------------------------------------


def _ask(prompt: str, default: str | None = None) -> str:
    """Read one answer, honouring `cancel` and an optional default."""
    suffix = f" {DIM}[{default}]{RESET}" if default else ""
    try:
        raw = input(f"{BLUE}{BOLD}{prompt}{RESET}{suffix}\n{BLUE}>{RESET} ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        raise Cancelled from None

    if raw.lower() in {"cancel", "quit", "exit", ":q"}:
        raise Cancelled
    if not raw and default is not None:
        return default
    return raw


def _ok(message: str) -> None:
    print(f"  {GREEN}{TICK}{RESET} {message}")


def _bad(answer: slots.Answer) -> None:
    """Show a hard failure and why the answer cannot be used."""
    print(f"  {RED}{CROSS} {answer.error}{RESET}")
    if answer.hint:
        for line in answer.hint.splitlines():
            print(f"    {DIM}{line}{RESET}" if line else "")


def _warn_now(messages: list[str]) -> None:
    for message in messages:
        print(f"  {YELLOW}{WARN}{RESET} {message}")


def _slot(
    prompt: str,
    validator: Callable[[str], slots.Answer],
    *,
    default: str | None = None,
    on_accept: Callable[[slots.Answer], str] | None = None,
    spec: Spec | None = None,
) -> Any:
    """Ask until the answer validates, then return its value.

    The loop is the whole contract of a hard error: an answer that names
    something which does not exist is never written into the spec, so the YAML
    cannot carry it.
    """
    while True:
        answer = validator(_ask(prompt, default))
        if not answer.ok:
            _bad(answer)
            continue

        _ok(on_accept(answer) if on_accept else str(answer.value))
        if answer.warnings:
            _warn_now(answer.warnings)
            if spec is not None:
                spec.note(answer.warnings)
        return answer.value


# --------------------------------------------------------------------------
# The interview
# --------------------------------------------------------------------------


def _ask_integration(spec: Spec) -> capabilities.Integration:
    """Settle the source->target pair first, against the live registry."""
    print(f"\n{BOLD}1. Integration{RESET}  {DIM}what moves where{RESET}")
    print(f"{DIM}Supported:{RESET}")
    for line in capabilities.describe_support().splitlines():
        print(f"{DIM}{line}{RESET}")

    sources = capabilities.known_sources()
    spec.source_system = _slot(
        "Source system",
        slots.validate_source_system,
        default=sources[0] if len(sources) == 1 else None,
    )

    targets = capabilities.targets_for(spec.source_system)
    spec.target_system = _slot(
        "Destination system",
        lambda raw: slots.validate_target_system(raw, spec.source_system),
        default=targets[0] if len(targets) == 1 else None,
    )

    integration = capabilities.find_integration(spec.source_system, spec.target_system)
    assert integration is not None  # the validator already proved this
    spec.blueprint = integration.blueprint
    _ok(f"blueprint {integration.blueprint}")
    return integration


def _ask_source(spec: Spec, grounding: Grounding) -> None:
    """Settle the connection and the source table against introspection."""
    print(f"\n{BOLD}2. Source{RESET}  {DIM}which database, which table{RESET}")
    for conn_id, info in grounding.connections.items():
        print(f"{DIM}  {conn_id}: {info['type']} -> {info['database']} on {info['host']}{RESET}")

    conn_ids = sorted(grounding.connections)
    spec.conn_id = _slot(
        "Source database connection",
        lambda raw: slots.validate_connection(raw, grounding.connections),
        default=conn_ids[0] if len(conn_ids) == 1 else None,
        on_accept=lambda a: (
            f"{a.value} -> {grounding.connections[a.value]['database']}"
        ),
    )

    tables = sorted(f"{s}.{t}" for s, t in grounding.catalog)
    print(f"{DIM}  tables: {', '.join(tables) if tables else '(none found)'}{RESET}")

    def accepted(answer: slots.Answer) -> str:
        schema, table = answer.value
        entry = grounding.catalog[(schema, table)]
        rows = entry["approx_rows"]
        count = "unknown rows" if rows is None else f"~{rows:,} rows"
        return f"{schema}.{table} ({count}, {len(entry['columns'])} columns)"

    spec.schema_name, spec.table_name = _slot(
        "Source table",
        lambda raw: slots.validate_table(raw, grounding.catalog),
        on_accept=accepted,
        spec=spec,
    )


def _ask_destination(spec: Spec) -> None:
    """Settle where the data lands."""
    print(f"\n{BOLD}3. Destination{RESET}  {DIM}where it lands in {spec.target_system}{RESET}")

    suggested = _suggest_namespace()
    spec.iceberg_namespace = _slot(
        f"{spec.target_system.capitalize()} namespace",
        lambda raw: slots.validate_identifier(raw, "namespace"),
        default=suggested,
    )
    spec.iceberg_table_name = _slot(
        f"{spec.target_system.capitalize()} table name",
        lambda raw: slots.validate_identifier(raw, "table name"),
        default=spec.table_name,
    )


def _ask_load(spec: Spec, integration: capabilities.Integration) -> None:
    """Settle write mode and read batch size."""
    print(f"\n{BOLD}4. Load behaviour{RESET}  {DIM}how data is written{RESET}")
    print(f"{DIM}  overwrite: drop and recreate the table each run{RESET}")
    print(f"{DIM}  append:    add rows, keeping what is already there{RESET}")

    spec.mode = _slot("Write mode", slots.validate_mode, default="overwrite", spec=spec)

    default_batch = integration.config_fields.get("batch_size", {}).get("default") or 10000
    spec.batch_size = _slot(
        "Rows per read batch",
        lambda raw: slots.validate_batch_size(raw, int(default_batch)),
        default=str(default_batch),
        spec=spec,
    )


def _ask_schedule(spec: Spec) -> None:
    """Settle the schedule, start date, dag_id and description."""
    print(f"\n{BOLD}5. Schedule{RESET}  {DIM}when it runs{RESET}")
    print(f"{DIM}  @daily, @hourly, @once, a cron expression, or 'none' for manual{RESET}")

    spec.schedule = _slot(
        "Schedule",
        slots.validate_schedule,
        default="@daily",
        on_accept=lambda a: "none (manual trigger only)" if a.value is None else str(a.value),
        spec=spec,
    )
    spec.start_date = _slot(
        "Start date", slots.validate_start_date, default="2024-01-01", spec=spec
    )

    print(f"\n{BOLD}6. Naming{RESET}")
    spec.dag_id = _slot(
        "DAG id", slots.validate_dag_id, default=_suggest_dag_id(spec)
    )
    # Collapsed to a single line here, so both the YAML comment and the quoted
    # `description` scalar stay well-formed whatever was typed.
    spec.description = " ".join(
        _ask(
            "Description",
            f"Ingest {spec.schema_name}.{spec.table_name} from "
            f"{spec.source_system} into {spec.target_system}",
        ).split()
    )


def _existing_dag_ids() -> set[str]:
    """dag_ids already on disk.

    Read from `DAGS_DIR` - the same constant `slots.validate_dag_id` checks
    against - so a suggested default can never be one the validator will then
    reject.
    """
    return {path.name.removesuffix(".dag.yaml") for path in DAGS_DIR.glob("*.dag.yaml")}


def _suggest_namespace() -> str:
    """Reuse the namespace the existing DAGs already use, when there is one.

    Takes the most common rather than the first, so one outlier DAG does not set
    the default for every pipeline after it.
    """
    from collections import Counter

    counts: Counter[str] = Counter()
    for path in sorted(DAGS_DIR.glob("*.dag.yaml")):
        try:
            doc = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError:
            continue
        for step in (doc.get("steps") or {}).values():
            if isinstance(step, dict) and step.get("iceberg_namespace"):
                counts[step["iceberg_namespace"]] += 1

    if not counts:
        return "default"
    return counts.most_common(1)[0][0]


def _suggest_dag_id(spec: Spec) -> str:
    """Propose a dag_id following the repo convention, avoiding collisions."""
    base = f"{spec.iceberg_namespace}_{spec.iceberg_table_name}_to_{spec.target_system}"
    existing = _existing_dag_ids()
    if base not in existing:
        return base
    for suffix in range(2, 100):
        candidate = f"{base}_v{suffix}"
        if candidate not in existing:
            return candidate
    return base


# --------------------------------------------------------------------------
# YAML assembly
# --------------------------------------------------------------------------


def build_yaml(spec: Spec) -> str:
    """Render the spec as Blueprint DAG YAML.

    Written by hand rather than with `yaml.dump` so the output matches the
    existing DAG files - key order, the leading `---`, quoted scalars, and a
    comment header - which is what makes a generated file reviewable next to a
    hand-written one.

    The step body is specific to `postgres_to_iceberg_blueprint`, the only
    blueprint that exists. A second blueprint with different config fields needs
    its own renderer here; `validate_dag_yaml` would reject a mismatch rather
    than let it reach the dags folder, so the failure is loud.
    """
    schedule = "null" if spec.schedule is None else f'"{spec.schedule}"'
    step_id = f"ingest_{spec.table_name}_table"

    # The description is free text, so it cannot be dropped into a quoted scalar
    # unescaped - one double quote would otherwise end the string early and
    # produce a file that does not parse.
    description = spec.description.replace("\\", "\\\\").replace('"', '\\"')

    return "\n".join(
        [
            "---",
            f"# {spec.description or spec.dag_id}",
            f"dag_id: {spec.dag_id}",
            f'description: "{description}"',
            f"schedule: {schedule}",
            f'start_date: "{spec.start_date}"',
            "catchup: false",
            "",
            "steps:",
            f"  {step_id}:",
            f"    blueprint: {spec.blueprint}",
            f"    table_name: {spec.table_name}",
            f"    schema_name: {spec.schema_name}",
            f"    iceberg_namespace: {spec.iceberg_namespace}",
            f"    iceberg_table_name: {spec.iceberg_table_name}",
            f"    s3_bucket: {spec.s3_bucket}",
            f"    s3_prefix: {spec.s3_prefix}",
            f"    mode: {spec.mode}",
            f"    batch_size: {spec.batch_size}",
            f"    postgres_conn_id: {spec.conn_id}",
            "",
        ]
    )


def _summarize(spec: Spec, text: str) -> None:
    """Show the finished spec, its warnings, and the YAML."""
    print(f"\n{BOLD}Summary{RESET}")
    rows = [
        ("Integration", f"{spec.source_system} -> {spec.target_system}"),
        ("Blueprint", spec.blueprint),
        ("Source", f"{spec.schema_name}.{spec.table_name} via {spec.conn_id}"),
        ("Destination", f"{spec.iceberg_namespace}.{spec.iceberg_table_name}"),
        ("Mode", spec.mode),
        ("Batch size", f"{spec.batch_size:,}"),
        ("Schedule", "none (manual)" if spec.schedule is None else spec.schedule),
        ("Start date", spec.start_date),
        ("DAG id", spec.dag_id),
        ("File", f"airflow/dags/{spec.dag_id}.dag.yaml"),
    ]
    width = max(len(label) for label, _ in rows)
    for label, value in rows:
        print(f"  {DIM}{label.ljust(width)}{RESET}  {value}")

    if spec.warnings:
        print(f"\n{YELLOW}{BOLD}Warnings ({len(spec.warnings)}){RESET} "
              f"{DIM}- these do not block the pipeline{RESET}")
        for warning in spec.warnings:
            print(f"  {YELLOW}{WARN}{RESET} {warning}")

    print(f"\n{BOLD}airflow/dags/{spec.dag_id}.dag.yaml{RESET}")
    print(f"{DIM}{'-' * 60}{RESET}")
    print(text)
    print(f"{DIM}{'-' * 60}{RESET}")


def run_interview(
    *,
    conn: SourceConnection | None = None,
    schemas: tuple[str, ...] = ("public",),
    grounding: Grounding | None = None,
) -> int:
    """Run the guided interview end to end."""
    if grounding is None:
        print(f"\n{DIM}Reading blueprints, source schema and connections...{RESET}")
        try:
            grounding = gather_grounding(conn, schemas)
        except ModuleNotFoundError as exc:
            # A missing driver is an install problem, not a database that is down,
            # and "is it running?" would send the user looking in the wrong place.
            print(f"{RED}Missing dependency:{RESET} {exc}")
            print(f"{DIM}Install the project dependencies first: uv sync{RESET}")
            return 1
        except Exception as exc:
            print(f"{RED}Could not read the source database:{RESET} {exc}")
            print(f"{DIM}Is it running? Try: make docker-up{RESET}")
            return 1

    if not capabilities.supported_integrations():
        print(f"{RED}No blueprints are installed, so no pipeline can be built.{RESET}")
        return 1

    if not grounding.catalog:
        print(f"{RED}No tables found in schema(s) {', '.join(schemas)}.{RESET}")
        print(f"{DIM}Seed the source database first: make seed-data{RESET}")
        return 1

    print(f"\n{BOLD}{CYAN}Pipeline wizard{RESET} "
          f"{DIM}- I'll ask for each parameter and check it as we go.{RESET}")
    print(f"{DIM}Press Enter to accept a [default]. Type 'cancel' to stop.{RESET}")

    spec = Spec()
    try:
        integration = _ask_integration(spec)
        _ask_source(spec, grounding)
        _ask_destination(spec)
        _ask_load(spec, integration)
        _ask_schedule(spec)

        text = build_yaml(spec)

        # The same validator the LLM path uses. The wizard checked every answer
        # on the way in, so a failure here means the assembly itself is wrong -
        # worth surfacing rather than trusting the slot checks alone.
        validation = validate_dag_yaml(text, spec.dag_id, f"airflow/dags/{spec.dag_id}.dag.yaml")
        _summarize(spec, text)

        if not validation.ok:
            print(f"\n{RED}{BOLD}The assembled YAML failed validation{RESET}")
            for error in validation.errors:
                print(f"  {RED}{CROSS}{RESET} {error}")
            print(f"{DIM}Refusing to write it. This is a bug in the wizard, not your input.{RESET}")
            return 1

        print(f"\n{GREEN}{TICK} Valid{RESET} {DIM}- checked against the real blueprint config "
              f"model and the dags folder.{RESET}")

        answer = _ask("Write this file? (yes / no)", "yes").lower()
        if answer not in {"y", "yes"}:
            print(f"{DIM}Not written.{RESET}")
            return 0

        path = write_dag(text, f"airflow/dags/{spec.dag_id}.dag.yaml", REPO_ROOT)
    except Cancelled:
        print(f"{DIM}Cancelled. Nothing was written.{RESET}")
        return 0
    except FileExistsError as exc:
        print(f"{RED}{exc}{RESET}")
        return 1

    print(f"{GREEN}Wrote{RESET} {path}")
    print(f"{DIM}The scheduler picks it up on its next scan.{RESET}")
    return 0
