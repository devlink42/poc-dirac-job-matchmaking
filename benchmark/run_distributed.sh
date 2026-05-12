#!/bin/bash
# Script to run Locust in distributed mode

# Default value
WORKERS=5
MODE=""
LOCUST_ARGS=""
NUM_JOBS=10000000
NUM_NODES=20000
CANDIDATES_COUNT=100
U_VAL=100
R_VAL=50

while [[ $# -gt 0 ]]; do
  case "$1" in
    --headless)
      MODE="--headless"
      shift
      ;;
    -u)
      U_VAL="$2"
      LOCUST_ARGS="$LOCUST_ARGS $1 $2"
      shift 2
      ;;
    -r)
      R_VAL="$2"
      LOCUST_ARGS="$LOCUST_ARGS $1 $2"
      shift 2
      ;;
    -t)
      LOCUST_ARGS="$LOCUST_ARGS $1 $2"
      shift 2
      ;;
    -w|--workers)
      WORKERS="$2"
      shift 2
      ;;
    *)
      LOCUST_ARGS="$LOCUST_ARGS $1"
      shift
      ;;
  esac
done

if [[ "$MODE" == "--headless" ]]; then
  if [[ ! "$LOCUST_ARGS" =~ "-u" ]]; then
    echo "No load parameters detected. Using default values: -u 100 -r 50 -t 5m"
    LOCUST_ARGS="-u 100 -r 50 -t 5m"
    U_VAL=100
    R_VAL=50
  fi

  CURRENT_DATE=$(date +"%Y-%m-%d_%H-%M-%S")
  CSV_PREFIX="benchmark/results/locust_${CURRENT_DATE}_jobs-${NUM_JOBS}_nodes-${NUM_NODES}_cc-${CANDIDATES_COUNT}_u-${U_VAL}_r-${R_VAL}"
  HTML_PREFIX="benchmark/results/html/locust_${CURRENT_DATE}_jobs-${NUM_JOBS}_nodes-${NUM_NODES}_cc-${CANDIDATES_COUNT}_u-${U_VAL}_r-${R_VAL}"

  echo "Starting Locust Master in HEADLESS mode with args: $LOCUST_ARGS"
  locust -f benchmark/locustfile.py --master --headless --num-jobs ${NUM_JOBS} --num-nodes ${NUM_NODES} --candidates-count ${CANDIDATES_COUNT} $LOCUST_ARGS --csv ${CSV_PREFIX} --csv-full-history --html ${HTML_PREFIX} &
else
  echo "Starting Locust Master with UI..."
  LOCUST_ARGS="-u 100 -r 50 -t 15m"
  locust -f benchmark/locustfile.py --master --num-jobs ${NUM_JOBS} --num-nodes ${NUM_NODES} --candidates-count ${CANDIDATES_COUNT} $LOCUST_ARGS &
fi
MASTER_PID=$!

# 4. Starting workers
echo "Starting $WORKERS Locust Workers..."
WORKER_PIDS=""
for _ in $(seq 1 "$WORKERS"); do
  locust -f benchmark/locustfile.py --worker &
  WORKER_PIDS="$WORKER_PIDS $!"
done

if [[ "$MODE" != "--headless" ]]; then
  echo "Distributed Locust is running! Go to http://localhost:8089"
  echo "Press [CTRL+C] to stop all processes."
else
  echo "Distributed Locust is running the headless benchmark..."
fi

# 5. Shutdown management (wait and cleanup)
trap 'echo -e "Stopping everything..."; kill $MASTER_PID $WORKER_PIDS 2>/dev/null; exit' SIGINT SIGTERM

wait $MASTER_PID
kill $WORKER_PIDS 2>/dev/null
echo "Benchmark finished."
