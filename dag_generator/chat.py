"""Interactive chat for the DAG generator.

A REPL around `generate_dag`, with two things a single-shot CLI call cannot do:

  * Grounding is gathered once and reused. Introspection queries the source
    database and the blueprint registry, so re-running it per turn would be slow
    and pointless within one session.
  * Turns are remembered, so "actually make it hourly" resolves against the DAG
    just proposed instead of being read as a fresh request.

Generated DAGs are held in memory until you `save` them, so an unsatisfying
first attempt costs nothing.

`new` hands over to the guided wizard in `wizard.py`, which asks for each
parameter and validates it locally instead of inferring the whole request at
once. Both modes share this session's grounding, so neither pays for
introspection twice.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from langchain_core.messages import AIMessage, HumanMessage

from . import capabilities
from .chain import GenerationResult, generate_dag
from .introspect import REPO_ROOT, Grounding, SourceConnection, gather_grounding
from .models import GenerationStatus
from .validate import write_dag
from .wizard import run_interview

logger = logging.getLogger(__name__)

GREEN, YELLOW, RED, BLUE, DIM, BOLD, RESET = (
    "\033[32m", "\033[33m", "\033[31m", "\033[34m", "\033[2m", "\033[1m", "\033[0m",
)

BANNER = f"""{BOLD}DAG generator{RESET} {DIM}- describe what you want to ingest{RESET}

  {DIM}e.g.{RESET} import the users table into iceberg
  {DIM}e.g.{RESET} load transactions hourly, append instead of overwrite

  {DIM}or let me walk you through it one question at a time:{RESET} {BOLD}new{RESET}

  {DIM}commands:{RESET} {BOLD}new{RESET}  guided wizard        {BOLD}save{RESET} write the last DAG
            {BOLD}tables{RESET} list source tables  {BOLD}dags{RESET} list existing DAGs
            {BOLD}support{RESET} what can be built  {BOLD}reset{RESET} forget this conversation
            {BOLD}help{RESET} show this            {BOLD}exit{RESET} quit
"""

HELP = BANNER


@dataclass
class Session:
    """One chat session: the grounding, the transcript, and the last result."""

    grounding: Grounding
    history: list
    last: GenerationResult | None = None

    @property
    def context(self) -> dict[str, str]:
        """The prompt blocks, which is all the LLM path needs."""
        return self.grounding.prompt_blocks

    def remember(self, request: str, result: GenerationResult) -> None:
        """Record a turn so follow-ups can refer to it.

        Only the YAML goes back as the assistant turn - replaying the whole
        grounding block for every past turn would bloat the prompt for no gain,
        since the current turn already carries it.
        """
        self.history.append(HumanMessage(content=request))
        reply = result.dag.yaml or result.dag.reasoning
        self.history.append(AIMessage(content=reply))
        self.last = result


def _render(result: GenerationResult) -> None:
    """Print one generation result."""
    dag = result.dag
    print(f"\n{BOLD}Reasoning{RESET}\n{dag.reasoning}\n")

    if dag.status is GenerationStatus.NEEDS_CLARIFICATION:
        print(f"{YELLOW}{BOLD}Needs clarification{RESET}")
        for question in dag.questions:
            print(f"  {YELLOW}?{RESET} {question}")
        print(f"\n{DIM}Answer above and I'll try again.{RESET}")
        return

    if result.validation is not None and not result.validation.ok:
        print(f"{RED}{BOLD}Validation failed after {result.attempts} attempt(s){RESET}")
        for error in result.validation.errors:
            print(f"  {RED}x{RESET} {error}")
        print(f"\n{DIM}--- last candidate ---{RESET}\n{dag.yaml}")
        return

    print(f"{BOLD}{dag.file_path}{RESET}  {GREEN}validated{RESET} "
          f"{DIM}({result.attempts} attempt(s)){RESET}\n")
    print(dag.yaml)

    if dag.warnings:
        print(f"{YELLOW}{BOLD}Warnings{RESET}")
        for warning in dag.warnings:
            print(f"  {YELLOW}!{RESET} {warning}")

    if result.written_to:
        print(f"\n{GREEN}Wrote{RESET} {result.written_to}")
    else:
        print(f"\n{DIM}Type 'save' to write it, or refine your request.{RESET}")


def _save(session: Session) -> None:
    """Write the last validated DAG to disk."""
    result = session.last
    if result is None:
        print(f"{YELLOW}Nothing to save yet.{RESET}")
        return
    if result.dag.status is not GenerationStatus.OK:
        print(f"{YELLOW}The last turn did not produce a DAG.{RESET}")
        return
    if result.validation is not None and not result.validation.ok:
        print(f"{RED}The last DAG failed validation; refusing to save it.{RESET}")
        return
    if result.written_to:
        print(f"{DIM}Already written to {result.written_to}.{RESET}")
        return

    try:
        path = write_dag(result.dag.yaml, result.dag.file_path, REPO_ROOT)
    except FileExistsError as exc:
        print(f"{RED}{exc}{RESET}")
        return

    result.written_to = path
    print(f"{GREEN}Wrote{RESET} {path}")
    print(f"{DIM}The scheduler picks it up on its next scan.{RESET}")


def _show(title: str, body: str) -> None:
    print(f"\n{BOLD}{title}{RESET}\n{body}\n")


def run(
    *,
    model: str | None = None,
    provider: str | None = None,
    conn: SourceConnection | None = None,
    schemas: tuple[str, ...] = ("public",),
    max_attempts: int = 3,
) -> int:
    """Run the chat loop until the user exits."""
    print(f"\n{DIM}Reading blueprints, source schema and existing DAGs...{RESET}")
    try:
        grounding = gather_grounding(conn, schemas)
    except Exception as exc:
        print(f"{RED}Could not gather grounding:{RESET} {exc}")
        print(f"{DIM}Is the source database running? Try: make docker-up{RESET}")
        return 1

    session = Session(grounding=grounding, history=[])
    print(BANNER)

    while True:
        try:
            raw = input(f"{BLUE}{BOLD}>{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0

        if not raw:
            continue

        command = raw.lower()
        if command in {"exit", "quit", ":q"}:
            return 0
        if command in {"help", "?"}:
            print(HELP)
            continue
        if command == "save":
            _save(session)
            continue
        if command in {"new", "wizard"}:
            # Reuses the grounding already read, so the interview starts instantly.
            run_interview(grounding=session.grounding, schemas=schemas)
            continue
        if command in {"support", "integrations"}:
            _show("Supported integrations", capabilities.describe_support())
            continue
        if command == "tables":
            _show("Source catalog", session.context["source_catalog"])
            continue
        if command == "dags":
            _show("Existing DAGs", session.context["existing_dags"])
            continue
        if command == "blueprints":
            _show("Blueprints", session.context["blueprints"])
            continue
        if command == "reset":
            session.history.clear()
            session.last = None
            print(f"{DIM}Conversation cleared. Grounding kept.{RESET}")
            continue

        try:
            result = generate_dag(
                raw,
                model=model,
                provider=provider,
                schemas=schemas,
                write=False,          # saving is always explicit in chat
                max_attempts=max_attempts,
                context=session.context,
                history=session.history,
            )
        except KeyboardInterrupt:
            print(f"\n{DIM}Cancelled.{RESET}")
            continue
        except Exception as exc:
            print(f"{RED}Generation failed:{RESET} {exc}")
            continue

        _render(result)
        session.remember(raw, result)
