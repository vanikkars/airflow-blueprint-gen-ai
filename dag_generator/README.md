# dag_generator — natural language to Airflow DAG

Turns *"import the transactions table into Iceberg"* into a validated
`*.dag.yaml` in `airflow/dags/`.

Two ways in. **Chat** takes the whole request at once and lets the model fill the
gaps. The **wizard** asks for one parameter at a time and validates each answer
against the live registry, catalog and connection list before moving on — no LLM
involved, so an unsupported integration or a table that does not exist is refused
immediately rather than after a round-trip.

```
request ──> gather grounding ──> prompt ──> LLM (structured) ──> validate ──> write
                 │                                                   │
                 │  blueprints · source catalog                      │ on failure:
                 │  existing dags · connections                      └─> repair loop
```

## Run it

From the repository root:

```bash
make gen          # chat: describe it in your own words (needs an LLM)
make wizard       # guided interview: one validated question at a time (no LLM)
```

### Guided wizard

```
make wizard
```

```
1. Integration  what moves where
Supported:
  postgres -> iceberg   (blueprint: postgres_to_iceberg_blueprint)
Source system [postgres]
> postgres
  ✓ postgres
Destination system [iceberg]
> snowflake
  ✗ UNSUPPORTED INTEGRATION: postgres -> snowflake
    From postgres, only these destinations exist: iceberg.
> iceberg
  ✓ iceberg
  ✓ blueprint postgres_to_iceberg_blueprint

2. Source  which database, which table
Source table
> accounts
  ✗ No table 'accounts' in the source database.
    Tables in the source database: public.transactions, public.users.
> public.users
  ✓ public.users (~100 rows, 5 columns)
  ⚠ Possible PII with no masking in the blueprint - these land in Iceberg
    in the clear: email, ssn.
```

It walks six steps — integration, source, destination, load behaviour, schedule,
naming — then shows a summary with every warning collected along the way and the
finished YAML. Nothing is written until you confirm.

Press Enter to take a `[default]`; type `cancel` to leave without writing.

### Chat

```
> import the users table into iceberg as customer_accounts
  ...proposed DAG, warnings...
> actually make it hourly and append instead of overwrite
  ...revised DAG...
> save
  Wrote airflow/dags/banking_customer_accounts_to_iceberg.dag.yaml
```

Grounding is read once at startup and turns are remembered, so a follow-up like
"make it hourly" revises the DAG just proposed instead of starting over. Nothing
is written until you type `save`.

Chat commands: `new` (hand over to the wizard), `save`, `tables`, `dags`,
`blueprints`, `support`, `reset`, `help`, `exit`. Both modes share one
introspection pass, so `new` starts instantly.

One-shot, for scripting:

```bash
python -m dag_generator.cli "import the transactions table into iceberg"
python -m dag_generator.cli "load users into iceberg hourly" --write -v
```

Without `--write` the DAG is printed and nothing touches the repo.

## Modules

| File | Role |
|---|---|
| `introspect.py` | Builds the four grounding blocks from live sources |
| `prompts/` | System prompt + per-request template |
| `models.py` | `GeneratedDag` — the structured-output contract |
| `chain.py` | `generate_dag()`: prompt → LLM → validate → repair → write |
| `chat.py` | Interactive REPL: one grounding pass, remembered turns, explicit save |
| `wizard.py` | Guided interview: slot-by-slot questions, local validation, no LLM |
| `slots.py` | Per-answer validators — hard errors re-ask, soft findings warn |
| `capabilities.py` | Which source→target pairs exist, derived from the registry |
| `validate.py` | Pre-write checks against the real config models and dags folder |
| `cli.py` | Terminal wrapper |

## How the wizard validates

Every answer is checked against something real, and failures fall into two kinds
that are handled differently.

**Hard errors re-ask.** The answer names something that does not exist, so no
correct YAML could contain it:

| Check | Against |
|---|---|
| Source to target integration | the blueprint registry |
| Airflow connection id | `airflow/config/airflow_connections.sh` |
| Source table and schema | live `information_schema` introspection |
| Write mode | `{overwrite, append}` - the only two implemented |
| Schedule | Airflow presets, or a 5-field cron with range-checked fields |
| `start_date` | ISO `YYYY-MM-DD`, normalized |
| `dag_id` | snake_case, and not already in `airflow/dags/` |

Unknown names come back with suggestions (`Did you mean transactions?`), and a
table that exists in two schemas is a question rather than a guess.

**Soft warnings are collected and shown at the summary.** The pipeline is
buildable; these are consequences worth seeing before it runs:

- Postgres types with no Iceberg mapping, which land as `StringType`
- `numeric`/`decimal` becoming `DoubleType`, so money loses exactness
- Column names that look like PII - the blueprint has no masking
- No primary key, so re-runs cannot deduplicate
- Large row counts, because the extract builds one in-memory DataFrame
- `overwrite`, which drops and recreates the table each run
- A null schedule, or a `start_date` in the future

Blocking on those would stop legitimate pipelines; hiding them would ship a
surprise. Nothing is written until you confirm at the summary.

### Unsupported integrations

`capabilities.py` derives the supported source-to-target pairs from the registry
rather than a hand-written list - from each blueprint's own `source`/`target`
attributes if it declares them, otherwise from the
`<source>_to_<target>_blueprint` naming convention. Asking for
`postgres -> snowflake` gets refused with what *does* exist, and dropping a new
blueprint into `airflow/blueprints/` adds its pair with no change here.

## Why the validator exists

The model is grounded, but grounding is not a guarantee. Every candidate is
checked before it can reach `airflow/dags/`, because Airflow imports the whole
folder through one `loader.py` — a single malformed file can take down the
entire dagbag, not just itself.

The checks:

- **Unknown step keys.** The blueprint config models do not set
  `extra="forbid"`, so Pydantic *silently drops* a key it doesn't know. An
  invented `incremental_column` would validate, run, and quietly perform a full
  overwrite. This check is the reason the validator exists.
- **Unknown top-level keys**, which do hard-fail at parse time.
- **`dag_id`** — lowercase snake_case, matching the filename, not already taken.
- **Unquoted `start_date`**, which YAML turns into a `date` object.
- **`mode`** outside `{overwrite, append}`; any other value falls through to append.

Failures go back to the model as specific errors (`ValidationResult.as_feedback`),
so it repairs its own output instead of regenerating blind. Default 3 attempts.

## Grounding is read live

Nothing about the blueprints, schema, or DAGs is hand-maintained in the prompt.
Add a blueprint to `airflow/blueprints/` and the generator can use it on the next
run. The source catalog annotates each column with how the blueprint will
actually treat it — unmapped types that become strings, `numeric` losing exact
precision as `DoubleType`, and PII that lands unmasked — so the model's warnings
are derived from the real schema rather than guessed.

## Configuration

### Model provider

Three backends, selected by `LLM_PROVIDER` in the project `.env` or `--provider`:

| Provider | Needs | Notes |
|---|---|---|
| `anthropic` (default) | `ANTHROPIC_API_KEY` | Simplest — just an API key |
| `bedrock` | AWS credentials in `infra/aws/.env` | Keeps traffic in your AWS account |
| `ollama` | A local Ollama server | Nothing leaves the machine |

### Local models with Ollama

```bash
uv sync --extra ollama
ollama pull qwen3:8b
python -m dag_generator.cli "import merchants into iceberg" --provider ollama
```

```bash
# .env
LLM_PROVIDER=ollama
OLLAMA_MODEL=qwen3:8b             # default
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_NUM_CTX=32768              # lower it on a small-RAM machine
```

**The model must support tool calling.** The generator uses
`with_structured_output`, which Ollama implements by constraining generation to
the schema; a model without that capability returns prose and every repair
attempt fails the same way. `ollama show <model>` lists capabilities, and the
provider refuses up front rather than letting you find out through three failed
attempts. `qwen3`, `llama3.1`+, `mistral-nemo` and `deepseek-r1` qualify;
`gemma3` does not.

**Context size matters here.** Ollama defaults to 2048 tokens and *silently
truncates* beyond it. The grounding block carries every table, column and type in
the source schema plus the full blueprint config schema, so the default would
drop exactly the catalog the model is meant to be grounded on — which is why
`num_ctx` is raised to 32768.

Expect local models to be slower and to need more repair attempts than Opus. The
validator is what makes this safe to try: a weaker model's mistakes are caught
against the real config models before anything reaches `airflow/dags/`, so the
failure mode is a rejected candidate, not a broken DAG. Raise `--max-attempts` if
a model is close but not quite landing it.

```bash
# .env
LLM_PROVIDER=bedrock
BEDROCK_MODEL_ID=us.anthropic.claude-sonnet-4-20250514-v1:0
BEDROCK_MAX_TOKENS=8000      # lower it for older models that cap below this
```

On Bedrock, credentials are read from **`infra/aws/.env`**, not the ambient
environment — the project-root `AWS_*` variables belong to MinIO and
`docker-compose` feeds them to Airflow. `chain.py` parses that file directly and
passes the credentials explicitly, and pins the Bedrock endpoint so a stray
`AWS_ENDPOINT_URL` cannot redirect the call to MinIO. See
[`infra/README.md`](../infra/README.md).

Model ids differ between providers: the Anthropic API uses `claude-opus-5`,
Bedrock uses inference profiles like
`us.anthropic.claude-sonnet-4-20250514-v1:0`, and Ollama uses local tags like
`qwen3:8b`. A Bedrock model must also be granted access in the AWS console
before it can be invoked.

### Source database

Introspection defaults to the compose banking database on `localhost:5433`.
Override with `GENAI_SOURCE_HOST`, `GENAI_SOURCE_PORT`, `GENAI_SOURCE_DB`,
`GENAI_SOURCE_USER`, `GENAI_SOURCE_PASSWORD`, `GENAI_AIRFLOW_CONN_ID`.

Note these are separate concerns: the generator connects *from your machine* to
read `information_schema`, while the `postgres_conn_id` written into the DAG is
resolved *inside the cluster* at runtime.

## Limits

- One blueprint exists, so every request resolves to Postgres→Iceberg. Requests
  for CDC, incremental watermarks, partitioning, or masking get a
  `needs_clarification` answer or an explicit warning — never a fabricated key.
  In the wizard, an unsupported pair is refused outright at the first question.
- The wizard builds one step per DAG. A multi-step pipeline is a chat request.
- The generator writes files; it does not unpause or trigger DAGs.
- `--write` refuses to overwrite an existing file; in chat, `save` does the same.
- Only schema metadata reaches the model — table and column names, types, keys
  and a row estimate. No `SELECT` is issued against your tables, so row values
  never leave the local stack. With `--provider ollama` nothing leaves the
  machine at all. Column *names* do go to a hosted provider, which is why `ssn` and
  `date_of_birth` appear in the prompt even though their values never do.
