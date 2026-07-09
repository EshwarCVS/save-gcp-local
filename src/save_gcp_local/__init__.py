"""save-gcp-local: run Airflow DAGs locally, execute Dataproc/Spark jobs in Docker.

Public API:
    from save_gcp_local import Config, SparkRunner, apply_patches
    from save_gcp_local.providers import get_provider, register, DataProvider
    from save_gcp_local.connectors import get_connector, resolve_connector, Connector
"""

from .config import Config, load_config
from .runner import SparkRunner
from .airflow_patch import apply_patches

__version__ = "0.3.0"
__all__ = ["Config", "load_config", "SparkRunner", "apply_patches", "__version__"]
