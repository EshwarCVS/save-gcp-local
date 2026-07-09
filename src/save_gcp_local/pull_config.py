"""YAML/JSON config loader for pull-data sources.

A config file defines multiple data sources to pull in one shot.  Format:

    # dpl-sources.yaml
    default_sample_size: 0.2       # applies to all sources unless overridden
    seed: 42

    sources:
      events:
        uri: gs://my-bucket/events/2024-06-01.csv
        dest: ./data/events/2024-06-01.csv
        sample_size: 0.3           # override for this source

      users:
        uri: postgresql://user:pass@host:5432/db
        table: users
        dest: ./data/users.parquet
        # inherits default_sample_size: 0.2

      transactions:
        uri: hive://hive-host:10000/default/transactions
        dest: ./data/transactions.csv

      raw_files:
        uri: s3://my-bucket/raw/
        dest: ./data/raw/
        sample_size: 1.0           # pull everything
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

log = logging.getLogger("save_gcp_local.pull_config")


@dataclass
class SourceSpec:
    name: str
    uri: str
    dest: str
    sample_size: Optional[float] = None
    table: Optional[str] = None
    query: Optional[str] = None
    limit: Optional[int] = None
    format: Optional[str] = None


@dataclass
class PullConfig:
    default_sample_size: float = 1.0
    seed: int = 42
    sources: List[SourceSpec] = field(default_factory=list)


def load_pull_config(path: str) -> PullConfig:
    """Load a pull-data config from YAML or JSON."""
    with open(path) as f:
        raw_text = f.read()

    if path.endswith(".json"):
        data = json.loads(raw_text)
    else:
        try:
            import yaml
            data = yaml.safe_load(raw_text)
        except ImportError:
            if path.endswith((".yaml", ".yml")):
                raise ImportError(
                    "PyYAML is required for YAML config files. "
                    "Install with: pip install pyyaml"
                )
            data = json.loads(raw_text)

    if not isinstance(data, dict):
        raise ValueError(f"Config file must be a dict/object, got {type(data).__name__}")

    config = PullConfig(
        default_sample_size=float(data.get("default_sample_size", 1.0)),
        seed=int(data.get("seed", 42)),
    )

    sources_raw: Dict = data.get("sources", {})
    for name, spec in sources_raw.items():
        if not isinstance(spec, dict):
            continue
        config.sources.append(SourceSpec(
            name=name,
            uri=spec.get("uri", ""),
            dest=spec.get("dest", ""),
            sample_size=float(spec["sample_size"]) if "sample_size" in spec else None,
            table=spec.get("table"),
            query=spec.get("query"),
            limit=int(spec["limit"]) if "limit" in spec else None,
            format=spec.get("format"),
        ))

    return config
