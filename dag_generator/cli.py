"""Command-line entry point for the DAG generator.

Interactive chat (no argument):

    python -m dag_generator.cli

Guided wizard - one question per parameter, each answer validated:

    python -m dag_generator.cli --wizard

Browser UI - the same chat, served on localhost:

    python -m dag_generator.cli --web

One-shot:

    python -m dag_generator.cli "import the transactions table into iceberg"
    python -m dag_generator.cli "import users into iceberg, hourly" --write
"""

from __future__ import annotations

import argparse
import logging
import sys

from .models import MAX_REPAIR_ATTEMPTS, GenerationStatus

# `chain` is imported lazily, inside the branch that needs it. Importing it at
# module scope would pull LangChain and a provider SDK into every invocation,
# including `--wizard`, which is deterministic and needs no model at all.

GREEN, YELLOW, RED, DIM, BOLD, RESET = (
    "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[1m", "\033[0m",
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dag_generator.cli",
        description="Generate an Airflow DAG from a natural-language ingestion request.",
    )
    parser.add_argument(
        "request",
        nargs="?",
        default=None,
        help=(
            "e.g. 'import the transactions table into iceberg'. "
            "Omit to start an interactive chat."
        ),
    )
    parser.add_argument(
        "--wizard",
        action="store_true",
        help=(
            "Guided interview: ask for each parameter and validate every answer "
            "before building the YAML. No LLM call is made."
        ),
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Serve the browser UI on localhost instead of running in the terminal.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Interface for --web to bind (default: 127.0.0.1, localhost only).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for --web (default: 8000).",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the --web server when its source changes.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the DAG into airflow/dags/ when it validates (default: print only).",
    )
    parser.add_argument(
        "--provider",
        choices=["anthropic", "bedrock", "ollama"],
        default=None,
        help=(
            "LLM backend (default: $LLM_PROVIDER, else anthropic). 'ollama' runs "
            "a local model, so nothing leaves this machine."
        ),
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Model id (default: the provider's own default).",
    )
    parser.add_argument(
        "--schema",
        action="append",
        dest="schemas",
        help="Postgres schema to introspect; repeatable (default: public).",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=MAX_REPAIR_ATTEMPTS,
        help=f"LLM calls allowed including repairs (default: {MAX_REPAIR_ATTEMPTS}).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Show progress logging.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format=f"{DIM}%(levelname)s %(message)s{RESET}",
    )

    # The browser UI is the chat loop with an HTTP front end, so it takes the
    # same provider settings and is checked before the terminal paths.
    if args.web:
        from .web.server import run as run_web

        return run_web(
            host=args.host,
            port=args.port,
            model=args.model,
            provider=args.provider,
            schemas=tuple(args.schemas or ("public",)),
            max_attempts=args.max_attempts,
            reload=args.reload,
        )

    # The wizard is deterministic - it asks, validates, and assembles the YAML
    # itself - so it needs no model or provider.
    if args.wizard:
        from .wizard import run_interview

        return run_interview(schemas=tuple(args.schemas or ("public",)))

    # No request means interactive mode: the chat loop owns its own output,
    # gathers grounding once, and saves only when asked.
    if args.request is None:
        from .chat import run

        return run(
            model=args.model,
            provider=args.provider,
            schemas=tuple(args.schemas or ("public",)),
            max_attempts=args.max_attempts,
        )

    from .chain import generate_dag

    # Introspection and provider setup both fail for ordinary, fixable reasons -
    # the source database is down, an API key is missing, Ollama is not running.
    # A traceback buries the one line that says which, so report the message and
    # let the chat loop's own handler deal with its interactive case.
    try:
        result = generate_dag(
            args.request,
            model=args.model,
            provider=args.provider,
            schemas=tuple(args.schemas or ("public",)),
            write=args.write,
            max_attempts=args.max_attempts,
        )
    except KeyboardInterrupt:
        print(f"\n{DIM}Cancelled.{RESET}")
        return 130
    except Exception as exc:
        print(f"{RED}{type(exc).__name__}:{RESET} {exc}")
        if "psycopg2" in type(exc).__module__:
            print(f"{DIM}Is the source database running? Try: make docker-up{RESET}")
        return 1

    dag = result.dag

    print(f"\n{BOLD}Reasoning{RESET}\n{dag.reasoning}\n")

    if dag.status is GenerationStatus.NEEDS_CLARIFICATION:
        print(f"{YELLOW}{BOLD}Needs clarification{RESET}")
        for question in dag.questions:
            print(f"  {YELLOW}?{RESET} {question}")
        return 2

    if result.validation is not None and not result.validation.ok:
        print(f"{RED}{BOLD}Validation failed after {result.attempts} attempt(s){RESET}")
        for error in result.validation.errors:
            print(f"  {RED}x{RESET} {error}")
        print(f"\n{DIM}--- last candidate ---{RESET}\n{dag.yaml}")
        return 1

    print(f"{BOLD}{dag.file_path}{RESET}  {GREEN}validated{RESET} "
          f"{DIM}({result.attempts} attempt(s)){RESET}\n")
    print(dag.yaml)

    if dag.warnings:
        print(f"\n{YELLOW}{BOLD}Warnings{RESET}")
        for warning in dag.warnings:
            print(f"  {YELLOW}!{RESET} {warning}")

    if result.written_to:
        print(f"\n{GREEN}Wrote{RESET} {result.written_to}")
    else:
        print(f"\n{DIM}Not written. Re-run with --write to save it.{RESET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
