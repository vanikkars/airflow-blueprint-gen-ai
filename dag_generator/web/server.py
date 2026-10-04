"""HTTP front end for the DAG generator.

The same thing `chat.py` is, with a browser instead of a terminal. It wraps
`generate_dag` and `write_dag` directly rather than reimplementing them, so the
web UI and `make gen` can never disagree about what validates or what gets
written.

Two pieces of state carry over from the chat loop, for the same reasons:

  * Grounding is gathered once, on the first request that needs it, and reused.
    Introspection queries the source database and the blueprint registry, so
    re-reading it per turn would be slow and pointless.
  * Turns are remembered per session, so "actually make it hourly" resolves
    against the DAG just proposed instead of being read as a fresh request.

Generation never writes to disk. The browser shows the YAML and `POST /api/save`
writes it only when the user asks, so an unsatisfying first attempt costs
nothing - the same contract the CLI's `save` command has.

This is a single-user development tool: it binds to localhost, holds sessions in
memory, and has no authentication. Do not expose it to a network.
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .. import capabilities
from ..chain import generate_dag
from ..introspect import REPO_ROOT, Grounding, gather_grounding
from ..models import MAX_REPAIR_ATTEMPTS, GenerationStatus
from ..validate import write_dag

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"

# Sessions are dropped oldest-first past this many. A browser that is reloaded
# repeatedly would otherwise accumulate transcripts for the life of the process.
MAX_SESSIONS = 32


@dataclass
class Session:
    """One browser conversation: the transcript and the last result.

    Mirrors `chat.Session`. The grounding is not held here because it is shared
    by every session in the process - it describes the repository and the source
    database, which no single conversation owns.
    """

    history: list = field(default_factory=list)
    last: Any = None  # GenerationResult | None

    def remember(self, request: str, result: Any) -> None:
        """Record a turn so follow-ups can refer to it.

        Only the YAML goes back as the assistant turn, exactly as in the chat
        loop: replaying the grounding for every past turn would bloat the prompt
        for no gain, since the current turn already carries it.
        """
        from langchain_core.messages import AIMessage, HumanMessage

        self.history.append(HumanMessage(content=request))
        self.history.append(AIMessage(content=result.dag.yaml or result.dag.reasoning))
        self.last = result


class Settings(BaseModel):
    """Provider settings for the whole server, fixed at startup."""

    model: str | None = None
    provider: str | None = None
    schemas: tuple[str, ...] = ("public",)
    max_attempts: int = MAX_REPAIR_ATTEMPTS


class GenerateRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64)
    request: str = Field(..., min_length=1, max_length=4000)


class SaveRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64)


def _dag_payload(result: Any) -> dict[str, Any]:
    """Flatten a GenerationResult into what the browser needs to render it.

    The four outcomes the UI distinguishes - needs_clarification, validation
    failure, validated, and already-written - are the same four the CLI's
    `_render` branches on.
    """
    dag = result.dag
    validation = result.validation

    payload: dict[str, Any] = {
        "status": dag.status.value,
        "reasoning": dag.reasoning,
        "attempts": result.attempts,
        "questions": list(dag.questions),
        "warnings": list(dag.warnings),
        "dag_id": dag.dag_id,
        "file_path": dag.file_path,
        "yaml": dag.yaml,
        "validated": bool(validation is not None and validation.ok),
        "errors": list(validation.errors) if validation is not None else [],
        "written_to": str(result.written_to) if result.written_to else None,
    }

    # `savable` is the server's verdict, not the browser's guess. The Save
    # button is enabled from this one flag so the rules about what may be
    # written live in one place.
    payload["savable"] = (
        dag.status is GenerationStatus.OK
        and payload["validated"]
        and not payload["written_to"]
    )
    return payload


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the FastAPI app.

    Grounding is read on first use rather than at import or startup, so the
    server comes up even when the source database is not running yet and
    reports the failure as a normal error the UI can show.
    """
    settings = settings or Settings()
    app = FastAPI(title="DAG generator", docs_url=None, redoc_url=None)

    state: dict[str, Any] = {"grounding": None, "sessions": {}}

    def grounding() -> Grounding:
        """The shared introspection pass, read once per process."""
        if state["grounding"] is None:
            logger.info("Gathering grounding (blueprints, source schema, existing DAGs)")
            state["grounding"] = gather_grounding(None, settings.schemas)
        return state["grounding"]

    def session(session_id: str) -> Session:
        sessions: dict[str, Session] = state["sessions"]
        if session_id not in sessions:
            if len(sessions) >= MAX_SESSIONS:
                sessions.pop(next(iter(sessions)))
            sessions[session_id] = Session()
        return sessions[session_id]

    @app.get("/api/bootstrap")
    def bootstrap() -> dict[str, Any]:
        """Everything the page needs to render before the first message.

        Introspection failures surface here as a readable message - a stopped
        source database is the single most likely reason this tool does not
        start, and the UI says so instead of showing an empty panel.
        """
        payload: dict[str, Any] = {
            "session_id": uuid.uuid4().hex,
            "provider": settings.provider or os.getenv("LLM_PROVIDER") or "anthropic",
            "model": settings.model,
            "support": capabilities.describe_support(),
            "ready": False,
            "error": None,
            "panels": {},
        }

        try:
            blocks = grounding().prompt_blocks
        except Exception as exc:
            payload["error"] = (
                f"{type(exc).__name__}: {exc}\n\n"
                "Is the source database running? Start it with: make docker-up"
            )
            return payload

        payload["ready"] = True
        payload["panels"] = {
            "tables": blocks["source_catalog"],
            "dags": blocks["existing_dags"],
            "blueprints": blocks["blueprints"],
            "connections": blocks["connections"],
        }
        return payload

    @app.post("/api/generate")
    def generate(body: GenerateRequest) -> dict[str, Any]:
        """Run one turn: request in, proposed DAG out. Never writes to disk."""
        try:
            context = grounding().prompt_blocks
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Could not read the source database ({type(exc).__name__}: {exc}). "
                    "Try: make docker-up"
                ),
            ) from exc

        current = session(body.session_id)

        try:
            result = generate_dag(
                body.request,
                model=settings.model,
                provider=settings.provider,
                schemas=settings.schemas,
                write=False,  # saving is always explicit, as in the CLI
                max_attempts=settings.max_attempts,
                context=context,
                history=current.history,
            )
        except Exception as exc:
            # A missing API key, an unreachable Ollama, a throttled Bedrock call:
            # all ordinary and fixable, and all unreadable as a traceback.
            logger.exception("Generation failed")
            raise HTTPException(
                status_code=502, detail=f"{type(exc).__name__}: {exc}"
            ) from exc

        current.remember(body.request, result)
        return _dag_payload(result)

    @app.post("/api/save")
    def save(body: SaveRequest) -> dict[str, Any]:
        """Write the session's last validated DAG into airflow/dags/.

        Re-checks every condition rather than trusting the button that called
        it: this endpoint writes a file the Airflow scheduler will import, and a
        stale tab is enough to send a request the UI would not offer.
        """
        current = session(body.session_id)
        result = current.last

        if result is None:
            raise HTTPException(status_code=400, detail="Nothing generated yet.")
        if result.dag.status is not GenerationStatus.OK:
            raise HTTPException(
                status_code=400, detail="The last turn did not produce a DAG."
            )
        if result.validation is None or not result.validation.ok:
            raise HTTPException(
                status_code=400,
                detail="The last DAG failed validation; refusing to save it.",
            )
        if result.written_to:
            return {
                "written_to": str(result.written_to),
                "relative_to": result.dag.file_path,
                "already": True,
            }

        try:
            path = write_dag(result.dag.yaml, result.dag.file_path, REPO_ROOT)
        except FileExistsError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        result.written_to = path
        logger.info("Wrote %s", path)
        return {
            "written_to": str(path),
            # The card shows the repo-relative path - the absolute one is mostly
            # the developer's home directory and wraps over several lines.
            "relative_to": result.dag.file_path,
            "already": False,
        }

    @app.post("/api/reset")
    def reset(body: SaveRequest) -> dict[str, Any]:
        """Forget the transcript, keep the grounding - the CLI's `reset`."""
        state["sessions"].pop(body.session_id, None)
        return {"session_id": uuid.uuid4().hex}

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


def run(
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    model: str | None = None,
    provider: str | None = None,
    schemas: tuple[str, ...] = ("public",),
    max_attempts: int = MAX_REPAIR_ATTEMPTS,
    reload: bool = False,
) -> int:
    """Serve the web UI until interrupted."""
    import uvicorn

    settings = Settings(
        model=model, provider=provider, schemas=schemas, max_attempts=max_attempts
    )

    GREEN, DIM, BOLD, RESET = "\033[32m", "\033[2m", "\033[1m", "\033[0m"
    print(f"\n{BOLD}DAG generator{RESET} {DIM}- web UI{RESET}")
    print(f"  {GREEN}>{RESET} http://{host}:{port}")
    print(f"  {DIM}provider: {provider or os.getenv('LLM_PROVIDER') or 'anthropic'}{RESET}")
    print(f"  {DIM}Ctrl-C to stop{RESET}\n")

    # `reload` needs an import string rather than an app object, and the
    # factory has to pick its settings up from the environment because the
    # reloader re-imports this module in a fresh process.
    if reload:
        os.environ["DAG_GENERATOR_WEB_SETTINGS"] = settings.model_dump_json()
        uvicorn.run(
            "dag_generator.web.server:_app_from_env",
            factory=True,
            host=host,
            port=port,
            reload=True,
        )
        return 0

    uvicorn.run(create_app(settings), host=host, port=port, log_level="warning")
    return 0


def _app_from_env() -> FastAPI:
    """App factory for `--reload`, which re-imports this module per worker."""
    raw = os.getenv("DAG_GENERATOR_WEB_SETTINGS")
    return create_app(Settings.model_validate_json(raw) if raw else Settings())
