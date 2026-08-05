#!/bin/bash
# Script to run Locust in various modes (local/distributed, UI/headless)

# Default values
MODE="headless"
PROFILE=false

LOCUST_ARGS=""
U_VAL=100
R_VAL=50
T_VAL=900  # 15min

NUM_JOBS=10000000
NUM_NODES=50000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --headless)
      MODE="headless"
      shift
      ;;
    --ui)
      MODE="ui"
      shift
      ;;
    --profile)
      PROFILE=true
      shift
      ;;
    -u|--users)
      U_VAL="$2"
      LOCUST_ARGS="$LOCUST_ARGS -u $2"
      shift 2
      ;;
    -r|--spawn-rate)
      R_VAL="$2"
      LOCUST_ARGS="$LOCUST_ARGS -r $2"
      shift 2
      ;;
    -t|--run-time)
      T_VAL="$2"
      LOCUST_ARGS="$LOCUST_ARGS -t $2"
      shift 2
      ;;
    --num-jobs)
      NUM_JOBS="$2"
      shift 2
      ;;
    --num-nodes)
      NUM_NODES="$2"
      shift 2
      ;;
    *)
      LOCUST_ARGS="$LOCUST_ARGS $1"
      shift
      ;;
  esac
done

CURRENT_DATE=$(date +"%Y-%m-%d_%H-%M-%S")
PREFIX_BASE="locust_${CURRENT_DATE}_jobs-${NUM_JOBS}_nodes-${NUM_NODES}_u-${U_VAL}_r-${R_VAL}_t-${T_VAL}"

CSV_PREFIX="benchmark/results/${PREFIX_BASE}"
HTML_PREFIX="benchmark/results/html/${PREFIX_BASE}.html"
FLAMEGRAPH_PATH="benchmark/results/html/svg/${PREFIX_BASE}_flamegraph.svg"

mkdir -p benchmark/results/html/svg

BASE_LOCUST_CMD="locust -f benchmark/locustfile.py --num-jobs ${NUM_JOBS} --num-nodes ${NUM_NODES}"

if [[ "$PROFILE" == true ]]; then
  echo "Profiling enabled. FlameGraph will be saved to: $FLAMEGRAPH_PATH"
  # We use --subprocesses to catch Locust workers if needed, and --idle to exclude waiting times
  BASE_LOCUST_CMD="py-spy record --format flamegraph -o ${FLAMEGRAPH_PATH} --subprocesses -- ${BASE_LOCUST_CMD}"
fi

if [[ ! "$LOCUST_ARGS" =~ "-u" ]]; then
  LOCUST_ARGS="$LOCUST_ARGS -u $U_VAL"
fi

if [[ ! "$LOCUST_ARGS" =~ "-r" ]]; then
  LOCUST_ARGS="$LOCUST_ARGS -r $R_VAL"
fi

if [[ ! "$LOCUST_ARGS" =~ "-t" ]]; then
  LOCUST_ARGS="$LOCUST_ARGS -t $T_VAL"
fi

if [[ "$MODE" == "headless" ]]; then
  echo "Running in HEADLESS mode with args: $LOCUST_ARGS"
  REPORT_ARGS="--csv ${CSV_PREFIX} --csv-full-history --html ${HTML_PREFIX}"
else
  echo "Running with UI..."
  REPORT_ARGS="--csv ${CSV_PREFIX} --csv-full-history --html ${HTML_PREFIX}"
fi


echo "Starting Standalone Locust..."
if [[ "$MODE" == "headless" ]]; then
  $BASE_LOCUST_CMD --headless $LOCUST_ARGS $REPORT_ARGS
else
  echo "Standalone Locust is running! Go to http://localhost:8089"
  $BASE_LOCUST_CMD $LOCUST_ARGS $REPORT_ARGS
fi

echo "Benchmark finished."
