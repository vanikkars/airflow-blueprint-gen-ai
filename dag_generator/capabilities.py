"""What this repository can actually build, derived from the live registry.

The wizard has to answer one question before it asks anything else: *is this
integration supported at all?* Answering it from a hand-written list would rot
the moment someone adds a blueprint, so the supported source->target pairs are
derived from the registry instead.

Two signals are used, in order:

  1. The blueprint's own `source`/`target` class attributes, if it declares them.
     A new blueprint can state its endpoints explicitly and nothing here needs
     to change.
  2. The blueprint *name*, which this repo spells `<source>_to_<target>_blueprint`.
     `postgres_to_iceberg_blueprint` therefore yields postgres -> iceberg.

A blueprint matching neither is still registered and still usable - it simply
cannot be offered as a source/target choice, which is better than guessing its
endpoints wrong and telling the user an integration exists when it does not.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache

from .introspect import BLUEPRINTS_DIR, DAGS_DIR

# Blueprint naming convention in this repo. The `_blueprint` suffix is optional
# so a future `postgres_to_iceberg` registers the same endpoints.
_NAME_PATTERN = re.compile(r"^(?P<source>[a-z0-9]+)_to_(?P<target>[a-z0-9]+)(?:_blueprint)?$")

# Spellings users reach for that are not the registered endpoint name. Kept
# deliberately small: it resolves synonyms for systems this repo supports, and
# must never map an unsupported system onto a supported one.
ALIASES = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "pg": "postgres",
    "psql": "postgres",
    "iceberg": "iceberg",
    "apache iceberg": "iceberg",
    "lakehouse": "iceberg",
}


@dataclass(frozen=True)
class Integration:
    """One source->target pair a registered blueprint can actually build."""

    source: str
    target: str
    blueprint: str
    description: str
    config_fields: dict[str, dict] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.source} -> {self.target}"


def _endpoints(name: str, cls: type) -> tuple[str, str] | None:
    """Work out the source and target a blueprint moves data between."""
    declared_source = getattr(cls, "source", None)
    declared_target = getattr(cls, "target", None)
    if isinstance(declared_source, str) and isinstance(declared_target, str):
        return declared_source.strip().lower(), declared_target.strip().lower()

    match = _NAME_PATTERN.match(name)
    if match:
        return match.group("source"), match.group("target")
    return None


@lru_cache(maxsize=1)
def supported_integrations() -> tuple[Integration, ...]:
    """Every source->target pair the installed blueprints can build.

    Cached: discovery imports every blueprint module, and the answer cannot
    change within one wizard session.
    """
    from blueprint import BlueprintRegistry

    registry = BlueprintRegistry(template_dirs=[BLUEPRINTS_DIR, DAGS_DIR])
    registry.discover()

    found: list[Integration] = []
    for entry in registry.list_blueprints():
        name = entry["name"]
        info = registry.get_blueprint_info(name)
        endpoints = _endpoints(name, registry.get(name))
        if endpoints is None:
            continue
        source, target = endpoints
        found.append(
            Integration(
                source=source,
                target=target,
                blueprint=name,
                description=(info["description"] or "").strip(),
                config_fields=info["parameters"],
            )
        )
    return tuple(found)


def normalize(system: str) -> str:
    """Fold a user's spelling of a system onto its canonical endpoint name."""
    cleaned = system.strip().lower()
    return ALIASES.get(cleaned, cleaned)


def known_sources() -> list[str]:
    """Source systems at least one blueprint can read from."""
    return sorted({i.source for i in supported_integrations()})


def known_targets() -> list[str]:
    """Target systems at least one blueprint can write to."""
    return sorted({i.target for i in supported_integrations()})


def find_integration(source: str, target: str) -> Integration | None:
    """The blueprint for this pair, or None when the pair is unsupported."""
    want_source, want_target = normalize(source), normalize(target)
    for integration in supported_integrations():
        if integration.source == want_source and integration.target == want_target:
            return integration
    return None


def targets_for(source: str) -> list[str]:
    """Targets reachable from one source, for an unsupported-pair message."""
    want = normalize(source)
    return sorted({i.target for i in supported_integrations() if i.source == want})


def sources_for(target: str) -> list[str]:
    """Sources that can reach one target, for an unsupported-pair message."""
    want = normalize(target)
    return sorted({i.source for i in supported_integrations() if i.target == want})


def describe_support() -> str:
    """A short, human-readable list of what can be built."""
    integrations = supported_integrations()
    if not integrations:
        return "No blueprints are installed, so no pipeline can be built."
    return "\n".join(
        f"  {i.source} -> {i.target}   (blueprint: {i.blueprint})" for i in integrations
    )
