"""The LangChain pipeline: request in, validated Airflow DAG YAML out.

Shape of the run:

    gather grounding -> prompt -> structured LLM call -> validate
                                        ^                    |
                                        +---- repair --------+

The repair loop is the part that matters. A single LLM call produces YAML that
usually parses; the loop is what makes the output trustworthy, because every
candidate is checked against the real blueprint config models and the real dags
folder before anyone sees it, and failures go back to the model as specific
errors rather than a generic retry.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from .introspect import REPO_ROOT, SourceConnection, gather_context
from .models import MAX_REPAIR_ATTEMPTS, GeneratedDag, GenerationStatus
from .validate import ValidationResult, validate_dag_yaml, write_dag

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

DEFAULT_MODEL = "claude-opus-5"

# Three backends. The Anthropic API is the default because it needs only an API
# key; Bedrock keeps the traffic inside an AWS account, which is what infra/aws/
# provisions access for; Ollama runs the model on this machine, so no request
# and no schema metadata leaves the host at all.
PROVIDER_ANTHROPIC = "anthropic"
PROVIDER_BEDROCK = "bedrock"
PROVIDER_OLLAMA = "ollama"

PROVIDERS = (PROVIDER_ANTHROPIC, PROVIDER_BEDROCK, PROVIDER_OLLAMA)

DEFAULT_BEDROCK_MODEL = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

# Ollama defaults. The model must support tool calling: the generator relies on
# `with_structured_output`, which Ollama implements by constraining generation to
# the schema, and a model without that capability returns prose instead of a
# GeneratedDag. `ollama show <model>` lists capabilities.
DEFAULT_OLLAMA_MODEL = "qwen3:8b"
DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"

# The grounding block is large - every table, column and type in the source
# schema, plus the full blueprint config schema. Ollama defaults to a 2048-token
# context and silently truncates beyond it, which drops the catalog the model is
# supposed to be grounded on. Raised here, overridable for small-RAM machines.
DEFAULT_OLLAMA_NUM_CTX = 32768

# Real AWS credentials live here, not in the project-root .env - that file
# defines the same AWS_* names for MinIO and docker-compose consumes them.
AWS_ENV_FILE = Path(__file__).resolve().parents[1] / "infra" / "aws" / ".env"


def _load_aws_env() -> dict[str, str]:
    """Read AWS_* settings from infra/aws/.env.

    Parsed rather than sourced, so no shell is involved and the values never
    leak into the wider environment. Tolerates the `export ` prefix and inline
    `unset` lines that file uses. Falls back to the ambient environment when the
    file is absent, which is what a CI runner with a real IAM role would want.
    """
    found: dict[str, str] = {}

    if AWS_ENV_FILE.is_file():
        for raw in AWS_ENV_FILE.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            line = line.removeprefix("export ").strip()
            key, _, value = line.partition("=")
            key = key.strip()
            if not key.startswith("AWS_"):
                continue
            value = value.strip().strip('"').strip("'")
            if value:
                found[key] = value

    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"):
        if key not in found and os.getenv(key):
            found[key] = os.environ[key]

    return found


def _check_ollama(base_url: str, model_id: str) -> None:
    """Fail early and specifically on the two common Ollama misconfigurations.

    Without this, a stopped server surfaces as a bare connection error and a
    model that was never pulled surfaces as a 404 from deep inside the client -
    neither of which says what to do. A model that cannot call tools is worse
    still: the request succeeds and returns prose, so `with_structured_output`
    fails to parse and the repair loop burns every attempt on the same problem.
    """
    import json
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(f"{base_url}/api/tags", timeout=5) as response:
            installed = json.load(response).get("models", []) or []
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(
            f"No Ollama server answering at {base_url} ({exc}). Start it with "
            "`ollama serve`, or point OLLAMA_BASE_URL at the right host."
        ) from exc

    names = {entry["name"] for entry in installed}
    # Ollama reports "llama3.1:latest" but accepts the bare "llama3.1".
    if model_id not in names and f"{model_id}:latest" not in names:
        available = ", ".join(sorted(names)) if names else "(none installed)"
        raise RuntimeError(
            f"Ollama has no model {model_id!r}. Pull it with `ollama pull {model_id}`.\n"
            f"Installed: {available}"
        )

    # Tool calling is what `with_structured_output` is built on, so a model
    # without it cannot produce a GeneratedDag no matter how good it is at YAML.
    for entry in installed:
        if entry["name"] in {model_id, f"{model_id}:latest"}:
            capabilities = entry.get("capabilities") or []
            # Older Ollama versions omit the field; absence is not a denial.
            if capabilities and "tools" not in capabilities:
                usable = sorted(
                    e["name"] for e in installed if "tools" in (e.get("capabilities") or [])
                )
                raise RuntimeError(
                    f"Ollama model {model_id!r} does not support tool calling, which "
                    "the generator needs for structured output - it would return prose "
                    "instead of a DAG.\n"
                    f"Models here that do: {', '.join(usable) if usable else '(none)'}. "
                    "Try `ollama pull qwen3:8b`."
                )
            break


def _resolve_provider(provider: str | None) -> str:
    """Pick the backend: explicit argument, then LLM_PROVIDER, else Anthropic."""
    chosen = (provider or os.getenv("LLM_PROVIDER") or PROVIDER_ANTHROPIC).lower()
    if chosen not in PROVIDERS:
        allowed = ", ".join(repr(p) for p in PROVIDERS)
        raise ValueError(f"Unknown provider {chosen!r}. Use one of {allowed}.")
    return chosen


@dataclass
class GenerationResult:
    """What a full run produced, including where it ended up on disk."""

    dag: GeneratedDag
    validation: ValidationResult | None
    attempts: int
    written_to: Path | None = None


def _load_prompt(name: str) -> str:
    return (PROMPTS_DIR / name).read_text()


def build_llm(
    model: str | None = None,
    temperature: float = 0.0,
    provider: str | None = None,
):
    """Construct the chat model for the chosen provider.

    Temperature 0 by default: this is code generation against a fixed schema,
    where reproducibility is worth more than variety.

    Args:
        model: Model id. Defaults to the provider's own default, since the
            Anthropic API and Bedrock use different id formats.
        temperature: Sampling temperature.
        provider: "anthropic", "bedrock" or "ollama". Defaults to
            $LLM_PROVIDER, then "anthropic".
    """
    resolved = _resolve_provider(provider)

    if resolved == PROVIDER_BEDROCK:
        # Imported lazily so the Anthropic path does not require boto3.
        from langchain_aws import ChatBedrockConverse

        creds = _load_aws_env()

        model_id = model or os.getenv("BEDROCK_MODEL_ID") or DEFAULT_BEDROCK_MODEL
        region = creds.get("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
        if not region:
            raise RuntimeError(
                f"No AWS region found. Set AWS_DEFAULT_REGION in {AWS_ENV_FILE}, "
                "matching the region infra/aws provisioned."
            )

        # Older/smaller Bedrock models cap output lower than the 8000 default
        # (claude-3-haiku tops out at 4096) and reject a larger request outright,
        # so allow an override.
        max_tokens = int(os.getenv("BEDROCK_MAX_TOKENS", "8000"))

        kwargs = {
            "model": model_id,
            "region_name": region,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        # Pass credentials explicitly rather than letting boto3 discover them.
        # The ambient AWS_* variables belong to MinIO (docker-compose feeds them
        # to Airflow and iceberg-rest), so relying on the default chain would
        # send "minioadmin" to Bedrock.
        if creds.get("AWS_ACCESS_KEY_ID") and creds.get("AWS_SECRET_ACCESS_KEY"):
            kwargs["aws_access_key_id"] = creds["AWS_ACCESS_KEY_ID"]
            kwargs["aws_secret_access_key"] = creds["AWS_SECRET_ACCESS_KEY"]

        # boto3 honours AWS_ENDPOINT_URL globally, so if MinIO's is exported in
        # this shell the Bedrock call is sent to http://minio:9000 and fails with
        # EndpointConnectionError. Pin the real Bedrock endpoint for this client.
        if os.getenv("AWS_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL_BEDROCK_RUNTIME"):
            kwargs["endpoint_url"] = f"https://bedrock-runtime.{region}.amazonaws.com"

        # Guardrails are optional; apply one only when the infrastructure
        # created it and the environment points at it.
        guardrail_id = os.getenv("BEDROCK_GUARDRAIL_ID")
        if guardrail_id:
            kwargs["guardrail_config"] = {
                "guardrailIdentifier": guardrail_id,
                "guardrailVersion": os.getenv("BEDROCK_GUARDRAIL_VERSION", "DRAFT"),
            }

        logger.info("Using Bedrock model %s in %s", model_id, region)
        return ChatBedrockConverse(**kwargs)

    if resolved == PROVIDER_OLLAMA:
        # Imported lazily so the hosted paths do not require langchain-ollama.
        from langchain_ollama import ChatOllama

        model_id = model or os.getenv("OLLAMA_MODEL") or DEFAULT_OLLAMA_MODEL
        base_url = os.getenv("OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL)
        num_ctx = int(os.getenv("OLLAMA_NUM_CTX", str(DEFAULT_OLLAMA_NUM_CTX)))

        _check_ollama(base_url, model_id)

        logger.info("Using Ollama model %s at %s (num_ctx=%d)", model_id, base_url, num_ctx)
        return ChatOllama(
            model=model_id,
            base_url=base_url,
            temperature=temperature,
            num_ctx=num_ctx,
        )

    from langchain_anthropic import ChatAnthropic

    if not os.getenv("ANTHROPIC_API_KEY"):
        raise RuntimeError(
            "ANTHROPIC_API_KEY is not set. Add it to .env or export it before "
            "running, or use --provider bedrock to go through AWS instead."
        )
    model_id = model or DEFAULT_MODEL
    logger.info("Using Anthropic API model %s", model_id)
    return ChatAnthropic(model=model_id, temperature=temperature, max_tokens=8000)


def generate_dag(
    user_request: str,
    *,
    model: str | None = None,
    provider: str | None = None,
    conn: SourceConnection | None = None,
    schemas: tuple[str, ...] = ("public",),
    write: bool = False,
    max_attempts: int = MAX_REPAIR_ATTEMPTS,
    context: dict[str, str] | None = None,
    history: list | None = None,
) -> GenerationResult:
    """Turn a natural-language ingestion request into a validated DAG.

    Args:
        user_request: e.g. "import the transactions table into iceberg".
        model: Model id; defaults to the provider's own default.
        provider: "anthropic", "bedrock" or "ollama". Defaults to $LLM_PROVIDER.
        conn: Source database to introspect; defaults to the compose banking DB.
        schemas: Postgres schemas to include in the catalog.
        write: Write the DAG into airflow/dags/ when it validates.
        max_attempts: Total LLM calls allowed, including repairs.
        context: Pre-gathered grounding blocks. Introspection hits the database
            and the blueprint registry, so a caller making several requests in a
            row (the chat loop) passes one context in rather than re-reading it.
        history: Prior turns, as alternating Human/AI messages. Lets a follow-up
            like "make it hourly" resolve against what was just generated.

    Returns:
        GenerationResult holding the DAG, the final validation, and the path
        written to when `write` is set.
    """
    context = context if context is not None else gather_context(conn, schemas)

    system = _load_prompt("dag_generator_system.md")
    human = _load_prompt("dag_generator_human.md").format(
        user_request=user_request, **context
    )

    # Structured output pins the response to GeneratedDag, so the field
    # descriptions in models.py act as part of the instructions and the reply
    # never needs to be parsed out of prose.
    llm = build_llm(model, provider=provider).with_structured_output(GeneratedDag)

    messages: list = [SystemMessage(content=system)]
    if history:
        # Earlier turns carry their own grounding, so only the newest request
        # needs the full context block appended.
        messages.extend(history)
    messages.append(HumanMessage(content=human))

    dag: GeneratedDag | None = None
    validation: ValidationResult | None = None

    for attempt in range(1, max_attempts + 1):
        logger.info("Generating DAG (attempt %d/%d)", attempt, max_attempts)
        dag = llm.invoke(messages)

        # A clarification request is a legitimate terminal answer: the model is
        # refusing to invent a table it cannot see, which is what we asked for.
        if dag.status is GenerationStatus.NEEDS_CLARIFICATION:
            logger.info("Model requested clarification; stopping.")
            return GenerationResult(dag=dag, validation=None, attempts=attempt)

        validation = validate_dag_yaml(dag.yaml, dag.dag_id, dag.file_path)
        if validation.ok:
            logger.info("DAG %s validated on attempt %d", dag.dag_id, attempt)
            break

        logger.warning(
            "Validation failed on attempt %d: %s", attempt, "; ".join(validation.errors)
        )
        if attempt < max_attempts:
            # Feed the failure back in place so the model repairs its own output
            # rather than regenerating from scratch and losing correct choices.
            messages.append(AIMessage(content=dag.yaml))
            messages.append(HumanMessage(content=validation.as_feedback()))

    assert dag is not None
    written: Path | None = None
    if write and validation is not None and validation.ok:
        written = write_dag(dag.yaml, dag.file_path, REPO_ROOT)
        logger.info("Wrote %s", written)

    return GenerationResult(
        dag=dag, validation=validation, attempts=attempt, written_to=written
    )
