"""Pluggable test-data providers.

Users choose how /data gets populated. The library ships three built-ins and a
clean base class so anyone can register their own. None of this is required —
if you stage data yourself, use provider "none".

Built-in providers
-------------------
  none        do nothing (you stage /data yourself)
  sample      copy a subset of real data (exact values preserved)
  synthetic   learn the statistical shape of real data, generate new rows

Custom providers
----------------
Subclass DataProvider, implement materialize(), and register it:

    from save_gcp_local.providers import register, DataProvider

    class MyProvider(DataProvider):
        name = "myprovider"
        def materialize(self, source, dest, **opts): ...

    register(MyProvider)
"""

from __future__ import annotations

import shutil
from typing import Dict, Type


class DataProvider:
    """Base class for test-data providers."""

    name: str = "base"

    def materialize(self, source: str, dest: str, **opts) -> str:
        """Produce test data at `dest` derived from `source`. Return dest path."""
        raise NotImplementedError


class NoneProvider(DataProvider):
    name = "none"

    def materialize(self, source: str, dest: str, **opts) -> str:
        # If a source is given, copy it verbatim; otherwise assume already staged.
        if source and source != dest:
            shutil.copy(source, dest)
        return dest


_REGISTRY: Dict[str, Type[DataProvider]] = {}


def register(provider_cls: Type[DataProvider]) -> Type[DataProvider]:
    _REGISTRY[provider_cls.name] = provider_cls
    return provider_cls


def get_provider(name: str) -> DataProvider:
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown data provider '{name}'. Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]()


def available() -> list:
    return sorted(_REGISTRY)


# Register built-ins. The pandas-backed ones import lazily so the core library
# has zero hard dependencies.
register(NoneProvider)

try:
    from .tabular import SampleProvider, SyntheticProvider  # noqa: F401
    register(SampleProvider)
    register(SyntheticProvider)
except Exception:
    # pandas/numpy not installed (the 'data' extra). Built-ins still work.
    pass
