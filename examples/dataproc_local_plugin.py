# Copy this file into $AIRFLOW_HOME/plugins/ to auto-load dataproc-local.
# It imports the plugin module, which applies the operator patches on startup.
#
# Configure via environment before booting Airflow, e.g.:
#   export DPL_JOBS_DIR=/path/to/spark-repo
#   export DPL_DATA_DIR=/path/to/test-data
#   export DPL_OUTPUT_DIR=/path/to/output
#   export DPL_ENABLED=true            # set false to passthrough to real GCP

from dataproc_local.airflow_plugin import *  # noqa: F401,F403
