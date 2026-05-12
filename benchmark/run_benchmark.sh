#!/bin/bash
# Script to run Locust in various modes (local/distributed, UI/headless)

# Default values
WORKERS=5
MODE="headless"
DISTRIBUTED=false

LOCUST_ARGS=""
U_VAL=100
R_VAL=50

NUM_JOBS=10000000
NUM_NODES=20000
CANDIDATES_COUNT=100

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
    --distributed)
      DISTRIBUTED=true
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
      LOCUST_ARGS="$LOCUST_ARGS -t $2"
      shift 2
      ;;
    -w|--workers)
      WORKERS="$2"
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
    --candidates-count)
      CANDIDATES_COUNT="$2"
      shift 2
      ;;
    *)
      LOCUST_ARGS="$LOCUST_ARGS $1"
      shift
      ;;
  esac
done

CURRENT_DATE=$(date + "%Y-%m-%d_%H-%M-%S")
CSV_PREFIX="benchmark/results/locust_${CURRENT_DATE}_jobs-${NUM_JOBS}_nodes-${NUM_NODES}_cc-${CANDIDATES_COUNT}_u-${U_VAL}_r-${R_VAL}"
HTML_PREFIX="benchmark/results/html/locust_${CURRENT_DATE}_jobs-${NUM_JOBS}_nodes-${NUM_NODES}_cc-${CANDIDATES_COUNT}_u-${U_VAL}_r-${R_VAL}.html"

mkdir -p benchmark/results/html

BASE_LOCUST_CMD="locust -f benchmark/locustfile.py --num-jobs ${NUM_JOBS} --num-nodes ${NUM_NODES} --candidates-count ${CANDIDATES_COUNT}"

if [[ "$MODE" == "headless" ]]; then
  if [[ ! "$LOCUST_ARGS" =~ "-u" ]]; then
    echo "No load parameters detected. Using default values: -u 100 -r 50 -t 15m"
    LOCUST_ARGS="-u 100 -r 50 -t 15m"
    U_VAL=100
    R_VAL=50
  fi

  echo "Running in HEADLESS mode with args: $LOCUST_ARGS"
  REPORT_ARGS="--csv ${CSV_PREFIX} --csv-full-history --html ${HTML_PREFIX}"
else
  echo "Running with UI..."
  if [[ ! "$LOCUST_ARGS" =~ "-u" ]]; then
    LOCUST_ARGS="-u 100 -r 50 -t 15m"
  fi

  REPORT_ARGS="--csv ${CSV_PREFIX} --csv-full-history --html ${HTML_PREFIX}"  # you can also generate reports in UI mode
fi

if [[ "$DISTRIBUTED" == true ]]; then
  echo "Starting Locust Master..."
  if [[ "$MODE" == "headless" ]]; then
    $BASE_LOCUST_CMD --master --headless $LOCUST_ARGS $REPORT_ARGS &
  else
    $BASE_LOCUST_CMD --master $LOCUST_ARGS $REPORT_ARGS &
  fi

  MASTER_PID=$!

  echo "Starting $WORKERS Locust Workers..."
  WORKER_PIDS=""
  for _ in $(seq 1 "$WORKERS"); do
    locust -f benchmark/locustfile.py --worker &
    WORKER_PIDS="$WORKER_PIDS $!"
  done

  if [[ "$MODE" != "headless" ]]; then
    echo "Distributed Locust is running! Go to http://localhost:8089"
    echo "Press [CTRL+C] to stop all processes."
  else
    echo "Distributed Locust is running the headless benchmark..."
  fi

  trap 'echo -e "\nStopping everything..."; kill $MASTER_PID $WORKER_PIDS 2>/dev/null; exit' SIGINT SIGTERM
  wait $MASTER_PID
  kill $WORKER_PIDS 2>/dev/null
else
  echo "Starting Standalone Locust..."
  if [[ "$MODE" == "headless" ]]; then
    $BASE_LOCUST_CMD --headless $LOCUST_ARGS $REPORT_ARGS
  else
    echo "Standalone Locust is running! Go to http://localhost:8089"
    $BASE_LOCUST_CMD $LOCUST_ARGS $REPORT_ARGS
  fi
fi

echo "Benchmark finished."
