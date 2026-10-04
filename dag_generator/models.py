"""Structured output contract for the DAG generator.

These models are handed to `with_structured_output`, so the field descriptions
are part of the prompt the model sees - they are documentation and instruction
at the same time.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field

# Total LLM calls allowed for one request, including validation repairs. It
# lives here rather than in `chain` so the CLI can use it as a parser default
# without importing LangChain - the wizard path needs neither.
MAX_REPAIR_ATTEMPTS = 3


class GenerationStatus(str, Enum):
    """Whether the generator could ground the request."""

    OK = "ok"
    NEEDS_CLARIFICATION = "needs_clarification"


class GeneratedDag(BaseModel):
    """The generator's verdict on one ingestion request."""

    status: GenerationStatus = Field(
        ...,
        description=(
            "'ok' when a DAG was produced. 'needs_clarification' when the request "
            "could not be grounded against the catalog or the available blueprints."
        ),
    )
    reasoning: str = Field(
        ...,
        description=(
            "Concise walkthrough: which table was resolved, which blueprint was "
            "chosen, and which config values were defaulted versus requested."
        ),
    )
    dag_id: str = Field(
        default="",
        description=(
            "snake_case DAG id, unique across the existing DAGs. Empty when status "
            "is 'needs_clarification'."
        ),
    )
    file_path: str = Field(
        default="",
        description=(
            "Repo-relative path, always 'airflow/dags/<dag_id>.dag.yaml'. Empty "
            "when status is 'needs_clarification'."
        ),
    )
    yaml: str = Field(
        default="",
        description=(
            "The complete DAG YAML file contents. Empty when status is "
            "'needs_clarification'."
        ),
    )
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "Things that are true and worth knowing even when the DAG is correct: "
            "type-mapping gaps, numeric precision loss, PII landing unmasked, "
            "destructive overwrite mode, memory pressure from large tables."
        ),
    )
    questions: list[str] = Field(
        default_factory=list,
        description=(
            "Specific questions to put to the user. Populated only when status is "
            "'needs_clarification'."
        ),
    )
