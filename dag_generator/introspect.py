"""Gather the facts the generator is allowed to reason from.

Every block this module produces is rendered into the prompt. If a fact is not
here, the model has no legitimate way to know it, which is the point: the
grounding rules tell the model to refuse rather than guess, and these functions
decide what "known" means.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
BLUEPRINTS_DIR = REPO_ROOT / "airflow" / "blueprints"
DAGS_DIR = REPO_ROOT / "airflow" / "dags"

# Postgres types the blueprint maps explicitly; anything else silently becomes a
# string in Iceberg. Kept in sync with the type_mapping in
# airflow/blueprints/postgres_to_iceberg.py.
MAPPED_PG_TYPES = {
    "integer",
    "bigint",
    "smallint",
    "real",
    "double precision",
    "numeric",
    "decimal",
    "text",
    "character varying",
    "varchar",
    "char",
    "character",
    "boolean",
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
}

# Types that survive the mapping but lose fidelity on the way.
LOSSY_PG_TYPES = {"numeric", "decimal"}

# Column-name fragments that suggest regulated or personal data. The blueprint
# has no masking, so anything matching lands in Iceberg in the clear.
SENSITIVE_PATTERNS = (
    "ssn",
    "social_security",
    "national_id",
    "tax_id",
    "passport",
    "email",
    "phone",
    "date_of_birth",
    "dob",
    "birth",
    "address",
    "zip_code",
    "postal",
    "credit_score",
    "card_number",
    "account_number",
    "iban",
    "password",
    "secret",
)


@dataclass(frozen=True)
class SourceConnection:
    """How to reach the source Postgres for introspection.

    This is the generator's own read-only connection for reading
    information_schema. It is deliberately separate from the Airflow connection
    id written into the DAG, which is resolved at DAG runtime inside the
    cluster and may use a different host.
    """

    host: str
    port: int
    database: str
    user: str
    password: str
    airflow_conn_id: str

    @classmethod
    def from_env(cls) -> SourceConnection:
        """Build from environment, defaulting to the compose banking database.

        The compose file publishes postgres-banking on host port 5433, which is
        how this process reaches it from outside the Airflow network.
        """
        return cls(
            host=os.getenv("GENAI_SOURCE_HOST", "localhost"),
            port=int(os.getenv("GENAI_SOURCE_PORT", "5433")),
            database=os.getenv("GENAI_SOURCE_DB", "bankingdb"),
            user=os.getenv("GENAI_SOURCE_USER", "bankinguser"),
            password=os.getenv("GENAI_SOURCE_PASSWORD", "bankingpass"),
            airflow_conn_id=os.getenv("GENAI_AIRFLOW_CONN_ID", "postgres_banking"),
        )


def describe_blueprints() -> str:
    """Render every registered blueprint with its full config schema.

    Read from the live BlueprintRegistry rather than a hand-written list, so a
    new blueprint dropped into airflow/blueprints/ becomes available to the
    generator without touching this file.
    """
    from blueprint import BlueprintRegistry

    registry = BlueprintRegistry(template_dirs=[BLUEPRINTS_DIR, DAGS_DIR])
    registry.discover()

    blocks: list[str] = []
    for entry in registry.list_blueprints():
        info = registry.get_blueprint_info(entry["name"])
        lines = [
            f"### {info['name']} (v{info['version']}, class {info['class']})",
            f"Description: {info['description'].strip()}",
            "",
            "Config fields - these are the ONLY keys allowed on a step using this blueprint:",
        ]
        for field, spec in info["parameters"].items():
            bits = [f"type={spec['type']}"]
            bits.append("REQUIRED" if spec["required"] else f"default={spec['default']!r}")
            if spec.get("enum"):
                bits.append(f"allowed={spec['enum']}")
            lines.append(f"  - {field}: {', '.join(bits)}")
            if spec["description"]:
                lines.append(f"      {spec['description']}")
        blocks.append("\n".join(lines))

    if not blocks:
        return "(No blueprints discovered - cannot generate any DAG.)"
    return "\n\n".join(blocks)


def read_source_catalog(
    conn: SourceConnection, schemas: tuple[str, ...] = ("public",)
) -> dict[tuple[str, str], dict[str, Any]]:
    """Introspect the source database into structured form.

    Returns a mapping of `(schema, table)` to a dict holding `columns`,
    `primary_key` and `approx_rows`. Two consumers need these facts in different
    shapes - the prompt wants prose, the wizard wants to look a table up by name
    and inspect its columns - so the query lives here once and each consumer
    renders from the result.
    """
    import psycopg2

    pg = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        database=conn.database,
        user=conn.user,
        password=conn.password,
        connect_timeout=10,
    )
    try:
        cur = pg.cursor()
        cur.execute(
            """
            SELECT c.table_schema, c.table_name, c.column_name, c.data_type,
                   c.is_nullable, c.character_maximum_length,
                   c.numeric_precision, c.numeric_scale
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE c.table_schema = ANY(%s) AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
            """,
            (list(schemas),),
        )
        columns = cur.fetchall()

        cur.execute(
            """
            SELECT tc.table_schema, tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tc.constraint_name
             AND kcu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = ANY(%s)
            """,
            (list(schemas),),
        )
        primary_keys: dict[tuple[str, str], set[str]] = {}
        for schema, table, column in cur.fetchall():
            primary_keys.setdefault((schema, table), set()).add(column)

        # reltuples is the planner's estimate, which avoids a full count on
        # large tables. It is -1 on a table that has never been analyzed.
        cur.execute(
            """
            SELECT n.nspname, c.relname, c.reltuples::bigint
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND n.nspname = ANY(%s)
            """,
            (list(schemas),),
        )
        row_counts = {(s, t): n for s, t, n in cur.fetchall()}
        cur.close()
    finally:
        pg.close()

    grouped: dict[tuple[str, str], list[Any]] = {}
    for row in columns:
        grouped.setdefault((row[0], row[1]), []).append(row)

    catalog: dict[tuple[str, str], dict[str, Any]] = {}
    for (schema, table), rows in sorted(grouped.items()):
        approx = row_counts.get((schema, table), -1)
        catalog[(schema, table)] = {
            "schema": schema,
            "table": table,
            # reltuples is -1 when the table has never been analyzed; None says
            # "unknown" rather than claiming a negative row count.
            "approx_rows": None if approx < 0 else approx,
            "primary_key": sorted(primary_keys.get((schema, table), set())),
            "columns": [
                {
                    "name": name,
                    "data_type": data_type,
                    "nullable": nullable == "YES",
                    "char_length": char_len,
                    "numeric_precision": num_prec,
                    "numeric_scale": num_scale,
                }
                for _, _, name, data_type, nullable, char_len, num_prec, num_scale in rows
            ],
        }
    return catalog


def _column_detail(column: dict[str, Any]) -> str:
    """Render a column's type with its length or precision."""
    data_type = column["data_type"]
    if column["char_length"]:
        return f"{data_type}({column['char_length']})"
    if column["numeric_precision"] and data_type in {"numeric", "decimal"}:
        return f"{data_type}({column['numeric_precision']},{column['numeric_scale'] or 0})"
    return data_type


def describe_source_catalog(conn: SourceConnection, schemas: tuple[str, ...] = ("public",)) -> str:
    """Render the source catalog as the prose block the prompt expects.

    Each column is annotated with how the blueprint will actually treat it, so
    the model can raise the hazards from its schema-review step without having
    to re-derive the type mapping.
    """
    return render_source_catalog(read_source_catalog(conn, schemas), schemas)


def render_source_catalog(
    catalog: dict[tuple[str, str], dict[str, Any]],
    schemas: tuple[str, ...] = ("public",),
) -> str:
    """Render an already-read catalog, so the wizard reuses one introspection pass."""
    if not catalog:
        return f"(No tables found in schema(s) {', '.join(schemas)}.)"

    blocks: list[str] = []
    for (schema, table), entry in sorted(catalog.items()):
        approx = entry["approx_rows"]
        approx_text = "unknown (never analyzed)" if approx is None else f"~{approx:,}"
        pks = set(entry["primary_key"])

        lines = [
            f"### {schema}.{table}",
            f"Approximate rows: {approx_text}",
            f"Primary key: {', '.join(sorted(pks)) if pks else '(none)'}",
            "Columns:",
        ]
        for column in entry["columns"]:
            name, data_type = column["name"], column["data_type"]

            notes: list[str] = []
            if name in pks:
                notes.append("PRIMARY KEY")
            notes.append("nullable" if column["nullable"] else "NOT NULL")
            if data_type not in MAPPED_PG_TYPES:
                notes.append("UNMAPPED -> falls back to Iceberg StringType")
            elif data_type in LOSSY_PG_TYPES:
                notes.append("maps to Iceberg DoubleType - loses exact precision")
            lowered = name.lower()
            if any(pattern in lowered for pattern in SENSITIVE_PATTERNS):
                notes.append("SENSITIVE - no masking in the blueprint")
            lines.append(f"  - {name}: {_column_detail(column)} [{'; '.join(notes)}]")
        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)


def describe_existing_dags() -> str:
    """Summarize the DAG YAML already in airflow/dags/.

    Supplies both the naming convention to follow and the dag_ids to avoid
    colliding with.
    """
    entries: list[str] = []
    for path in sorted(DAGS_DIR.glob("*.dag.yaml")):
        try:
            doc = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as exc:
            entries.append(f"- {path.name}: (unparseable: {exc})")
            continue

        dag_id = doc.get("dag_id", "(missing)")
        description = doc.get("description", "")
        schedule = doc.get("schedule", "")
        steps = doc.get("steps") or {}
        blueprints = sorted({s.get("blueprint", "?") for s in steps.values() if isinstance(s, dict)})
        namespaces = sorted(
            {s["iceberg_namespace"] for s in steps.values() if isinstance(s, dict) and "iceberg_namespace" in s}
        )

        line = (
            f"- {path.name}: dag_id={dag_id}, schedule={schedule!r}, "
            f"blueprints={blueprints}, iceberg_namespaces={namespaces}"
        )
        if description:
            line += f", description={description!r}"
        if dag_id != path.name.removesuffix(".dag.yaml"):
            line += "  <-- WARNING: dag_id does not match filename; do not copy this"
        entries.append(line)

    if not entries:
        return "(No existing DAGs.)"
    return "\n".join(entries)


def read_connections(conn: SourceConnection) -> dict[str, dict[str, str]]:
    """The Airflow connection ids a generated DAG may reference.

    Parsed from the provisioning script that actually creates them, so neither
    the prompt nor the wizard can drift from what Airflow really has.
    """
    script = REPO_ROOT / "airflow" / "config" / "airflow_connections.sh"
    found: dict[str, dict[str, str]] = {}
    if script.exists():
        text = script.read_text()
        for match in re.finditer(
            r"airflow connections add\s+'([^']+)'(.*?)(?=airflow connections add|\Z)",
            text,
            re.DOTALL,
        ):
            conn_id, body = match.group(1), match.group(2)

            def _flag(name: str, body: str = body) -> str:
                m = re.search(rf"--conn-{name}\s+'([^']*)'", body)
                return m.group(1) if m else "?"

            found[conn_id] = {
                "type": _flag("type"),
                "host": _flag("host"),
                "database": _flag("schema"),
                "assumed": "",
            }

    if not found:
        found[conn.airflow_conn_id] = {
            "type": "postgres",
            "host": conn.host,
            "database": conn.database,
            "assumed": "connection script not found",
        }
    return found


def describe_connections(conn: SourceConnection) -> str:
    """Render the connection list as the prose block the prompt expects."""
    return render_connections(read_connections(conn))


def render_connections(connections: dict[str, dict[str, str]]) -> str:
    """Render an already-read connection list, so the wizard reuses one pass."""
    lines: list[str] = []
    for conn_id, spec in connections.items():
        line = (
            f"- {conn_id}: type={spec['type']}, host={spec['host']}, "
            f"database={spec['database']}"
        )
        if spec.get("assumed"):
            line += f" (assumed; {spec['assumed']})"
        lines.append(line)
    return "\n".join(lines)


@dataclass(frozen=True)
class Grounding:
    """One introspection pass, in both the shapes its consumers need.

    `prompt_blocks` is what the prompt template interpolates. `catalog` and
    `connections` are the same facts structured, which is what the wizard
    validates answers against. Both come from a single pass so the wizard and
    the model can never disagree about what exists.
    """

    prompt_blocks: dict[str, str]
    catalog: dict[tuple[str, str], dict[str, Any]]
    connections: dict[str, dict[str, str]]
    source_conn: SourceConnection


def gather_grounding(
    conn: SourceConnection | None = None, schemas: tuple[str, ...] = ("public",)
) -> Grounding:
    """Introspect everything once, for both the prompt and the wizard."""
    conn = conn or SourceConnection.from_env()
    catalog = read_source_catalog(conn, schemas)
    connections = read_connections(conn)
    return Grounding(
        prompt_blocks={
            "blueprints": describe_blueprints(),
            "source_catalog": render_source_catalog(catalog, schemas),
            "existing_dags": describe_existing_dags(),
            "connections": render_connections(connections),
        },
        catalog=catalog,
        connections=connections,
        source_conn=conn,
    )


def gather_context(conn: SourceConnection | None = None, schemas: tuple[str, ...] = ("public",)) -> dict[str, str]:
    """Assemble every grounding block the prompt template expects."""
    return gather_grounding(conn, schemas).prompt_blocks
