"""LangChain automation that turns a natural-language request into an Airflow DAG.

The entry point is `generate_dag` in `chain.py`; `cli.py` wraps it for the
terminal. Everything else is grounding: `introspect.py` reads the blueprint
registry, the source database, the existing DAGs, and the Airflow connections,
so the model selects from what exists instead of inventing it.

Importing this package loads the project `.env`, because every setting the
generator reads - `LLM_PROVIDER`, the provider keys, the `OLLAMA_*` and
`GENAI_SOURCE_*` overrides - is documented as living there.
"""

import os
from pathlib import Path

from .models import GeneratedDag, GenerationStatus


def _load_env() -> None:
    """Load the project `.env` into the environment, if one exists.

    Done at import time so every entry point behaves the same: the CLI, the chat
    loop and the wizard all read configuration through `os.getenv`, and without
    this a `.env` setting such as `LLM_PROVIDER=ollama` would be silently
    ignored and the default provider used instead.

    Real environment variables win. An explicitly exported value is a
    deliberate override for one run, so `.env` must not clobber it - which is
    also what `load_dotenv` does by default.
    """
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if not env_file.is_file():
        return

    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        # python-dotenv is a transitive dependency, not a declared one. Fall
        # back to a minimal parser rather than failing to start over config
        # loading, which is not the job the user asked for.
        for raw in env_file.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.removeprefix("export ").partition("=")
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip().strip('"').strip("'")
        return

    load_dotenv(env_file)


_load_env()

__all__ = ["GeneratedDag", "GenerationStatus"]
