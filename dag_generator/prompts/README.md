# Pipeline generation prompts

`dag_generator_system.md` — the system prompt. Static; no format variables.
`dag_generator_human.md` — the per-request turn, filled by `chain.generate_dag`.

| Variable | Built by | Contents |
|---|---|---|
| `blueprints` | `introspect.describe_blueprints` | Live `BlueprintRegistry` discovery; name, description, and every config field with type/default/required |
| `source_catalog` | `introspect.describe_source_catalog` | `information_schema` + `pg_class.reltuples`; columns annotated with unmapped-type, precision-loss, and PII hazards |
| `existing_dags` | `introspect.describe_existing_dags` | Each `*.dag.yaml` with its dag_id, schedule, blueprints, namespaces, and a warning when dag_id ≠ filename |
| `connections` | `introspect.describe_connections` | Parsed from `airflow/config/airflow_connections.sh` |
| `user_request` | caller | Free text |

All grounding is derived at runtime from the live registry, database, and repo —
never hand-maintained here. The prompt's accuracy guarantee depends on that.
