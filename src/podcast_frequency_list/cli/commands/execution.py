from __future__ import annotations

from collections.abc import Callable
from functools import partial
from types import ModuleType
from typing import Any

from podcast_frequency_list.cli.runtime import run_bootstrapped_service_command

_REGISTRY: ModuleType | None = None
HandledErrors = type[Exception] | tuple[type[Exception], ...]
Emitter = Callable[[object], None]
CommandCallback = Callable[..., None]


def set_registry(registry: ModuleType) -> None:
    global _REGISTRY
    _REGISTRY = registry


def service_factory(factory_name: str) -> Callable[[], object]:
    if _REGISTRY is None:
        raise RuntimeError("cli command registry not initialized")
    return getattr(_REGISTRY, factory_name)


def run_service_method(
    *,
    service_factory_name: str,
    handled_errors: HandledErrors,
    emitter: Emitter,
    method_name: str,
    method_kwargs: dict[str, Any],
) -> None:
    run_bootstrapped_service_command(
        service_factory(service_factory_name),
        partial(
            call_service_method,
            emitter=emitter,
            method_name=method_name,
            method_kwargs=method_kwargs,
        ),
        handled_errors,
    )


def call_service_method(
    service: object,
    *,
    emitter: Emitter,
    method_name: str,
    method_kwargs: dict[str, Any],
) -> None:
    emitter(getattr(service, method_name)(**method_kwargs))
