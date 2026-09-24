"""AWS Glue (5.0, Spark 3.5) entry point. One Glue job runs any Spark stage.

Glue job args (set by Terraform / the Airflow GlueJobOperator):
  --stage_args   e.g. "silver --run-id 20250601T060000"
  --extra-py-files  s3://.../signals.zip  (the src/signals package, built by `make package`)
  --extra-files     the three config/*.yaml files
  SIGNALS_STORAGE_ROOT / SIGNALS_STATE_ROOT come from --conf / job env (s3://bucket/...)
"""
import os
import shlex
import sys

from awsglue.utils import getResolvedOptions  # type: ignore  # provided by the Glue runtime

os.environ["GLUE_JOB"] = "1"
args = getResolvedOptions(sys.argv, ["stage_args", "storage_root", "state_root"])
os.environ["SIGNALS_STORAGE_ROOT"] = args["storage_root"]
os.environ["SIGNALS_STATE_ROOT"] = args["state_root"]
# The YAML configs are shipped with --extra-files, which Glue drops into the working directory.
os.environ.setdefault("SIGNALS_CONFIG_DIR", os.getcwd())

from signals.cli import main  # noqa: E402

sys.exit(main(shlex.split(args["stage_args"])))
