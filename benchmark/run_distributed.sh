#!/bin/bash
# Script to run Locust in distributed mode

# Default value
WORKERS=5
MODE=""
LOCUST_ARGS=""
MATCH_MODE_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --headless)
      MODE="--headless"
      shift
      ;;
    -w|--workers)
      WORKERS="$2"
      shift 2
      ;;
    --match-mode)
      MATCH_MODE_ARG="--match-mode $2"
      shift 2
      ;;
    -u|-r|-t)
      LOCUST_ARGS="$LOCUST_ARGS $1 $2"
      shift 2
      ;;
    --num-jobs|--num-nodes)
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
    echo "No load parameters detected. Using default values: -u 100 -r 50 -t 15m"
    LOCUST_ARGS="-u 100 -r 50 -t 15m"
  fi

  echo "Starting Locust Master in HEADLESS mode with args: $LOCUST_ARGS"
  locust -f benchmark/locustfile.py --master --headless --num-jobs 10000000 --num-nodes 20000 --candidates-count 100 $LOCUST_ARGS &
else
  echo "Starting Locust Master with UI..."
  LOCUST_ARGS="-u 100 -r 50 -t 15m"
  locust -f benchmark/locustfile.py --master --num-jobs 10000000 --num-nodes 20000 --candidates-count 100 $LOCUST_ARGS &
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
