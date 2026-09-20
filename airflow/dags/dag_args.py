"""Project-wide DAG argument template.

The built-in DefaultDagArgs only accepts `schedule` and `description`, so the
top-level keys this project's DAG YAML uses (start_date, catchup, owner,
retries) would be rejected. This template declares them and maps them onto the
Airflow DAG constructor.
"""

from datetime import datetime, timedelta
from typing import Any

from blueprint import BlueprintDagArgs, BaseModel


class ProjectDagArgsConfig(BaseModel):
    schedule: str | None = None
    description: str | None = None
    start_date: str | None = None
    catchup: bool = False
    owner: str = "data-engineering"
    retries: int = 2
    retry_delay_minutes: int = 5


class ProjectDagArgs(BlueprintDagArgs[ProjectDagArgsConfig], default=True):
    """DAG arguments shared by every pipeline in this project."""

    def render(self, config: ProjectDagArgsConfig) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "catchup": config.catchup,
            "default_args": {
                "owner": config.owner,
                "retries": config.retries,
                "retry_delay": timedelta(minutes=config.retry_delay_minutes),
            },
        }
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description
        if config.start_date is not None:
            kwargs["start_date"] = datetime.fromisoformat(config.start_date)
        return kwargs
