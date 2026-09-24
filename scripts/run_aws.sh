#!/usr/bin/env bash
# Run the whole pipeline once on AWS: ingest (here) -> Glue Spark stages -> Athena.
# Works from AWS CloudShell or any shell with AWS credentials and `terraform apply` done.
#   bash scripts/run_aws.sh            # optional: GITHUB_TOKEN=... SIGNALS_MAX_PAGES=50
#   SKIP_INGEST=1 bash scripts/run_aws.sh   # re-run the Spark + Athena stages only
#   SKIP_INGEST=1 SKIP_SILVER=1 bash scripts/run_aws.sh   # resume from the gold-layer checks
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" PYTHONPATH=src
eval "$(cd infra/terraform && terraform output -raw env_exports)"
{ python3 -m pip install -q pyyaml requests boto3 || python3 -m pip install --user -q pyyaml requests boto3; } >/dev/null 2>&1
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
GLUE_MODULES="${GLUE_MODULES:-pyyaml,scikit-learn==1.5.2}"   # installed on the Glue workers at start
echo "== run $RUN_ID -> $SIGNALS_STORAGE_ROOT"

if [ -z "${SKIP_INGEST:-}" ]; then
  echo "== 1/7 ingest GitHub -> S3 bronze"
  python3 -m signals ingest --run-id "$RUN_ID" 2>&1 | grep -E 'github_issues:' || true
fi

glue() {
  local args="$1 --run-id $RUN_ID" id st
  # The job allows one run at a time; a just-finished run can hold the slot for a few
  # seconds, so retry ConcurrentRunsExceeded instead of failing.
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if id=$(aws glue start-job-run --job-name "$SIGNALS_GLUE_JOB" \
          --arguments "{\"--stage_args\":\"$args\",\"--additional-python-modules\":\"$GLUE_MODULES\"}" \
          --query JobRunId --output text 2>/tmp/glue_start.err); then break; fi
    grep -q ConcurrentRunsExceeded /tmp/glue_start.err || { cat /tmp/glue_start.err; exit 1; }
    sleep 15
  done
  echo "== glue: $1 ($id)"
  while :; do
    st=$(aws glue get-job-run --job-name "$SIGNALS_GLUE_JOB" --run-id "$id" --query JobRun.JobRunState --output text)
    case "$st" in
      SUCCEEDED) echo "   ok"; return 0 ;;
      FAILED|ERROR|TIMEOUT|STOPPED)
        echo "   $st: $(aws glue get-job-run --job-name "$SIGNALS_GLUE_JOB" --run-id "$id" \
                          --query JobRun.ErrorMessage --output text)"; exit 1 ;;
    esac
    sleep 15
  done
}
[ -z "${SKIP_SILVER:-}" ] && glue "silver"   # 2/7
glue "dq --layer silver --cloudwatch"  # 3/7
glue "gold"                            # 4/7
glue "dq --layer gold --cloudwatch"    # 5/7
glue "ml"                              # 6/7

echo "== 7/7 register gold in Athena + build mart views"
python3 -m signals load --target athena --run-id "$RUN_ID" | grep -A8 '"rows"' || true
echo "== done. Query it: Athena console -> workgroup $ATHENA_WORKGROUP -> database $ATHENA_DATABASE"
