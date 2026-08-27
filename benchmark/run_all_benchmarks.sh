#!/usr/bin/env bash

set -e

NUM_JOBS=10000000
NUM_NODES=50000
TIME="15m"

declare -a scenarios=(
    "Scenario_1_Baseline_NonDistrib             50     10   100  python        no"
    "Scenario_2_Baseline_Distrib                100    20   100  python        yes"
    "Scenario_3_HighCandidate_NonDistrib        50     10   500  python        no"
    "Scenario_4_HighCandidate_Distrib           50     10   500  python        yes"
    "Scenario_5_ExtremeLoad_Distrib             1000   100  50   python        yes"
    "Scenario_6_LikeReal_Distrib                10000  100  500  python        yes"
    "Scenario_7_LikeReal_NonDistrib             10000  100  500  python        no"
    "Scenario_1_redis_Baseline_NonDistrib       50     10   100  python_redis  no"
    "Scenario_2_redis_Baseline_Distrib          100    20   100  python_redis  yes"
    "Scenario_3_redis_HighCandidate_NonDistrib  50     10   500  python_redis  no"
    "Scenario_4_redis_HighCandidate_Distrib     50     10   500  python_redis  yes"
    "Scenario_5_redis_ExtremeLoad_Distrib       1000   100  50   python_redis  yes"
    "Scenario_6_redis_LikeReal_Distrib          10000  100  500  python_redis  yes"
    "Scenario_7_redis_LikeReal_NonDistrib       10000  100  500  python_redis  no"
)

echo "============================================================"
echo "Starting benchmark suite"
echo "============================================================"

for scenario in "${scenarios[@]}"; do
    read -r name users spawn cand_count mode dist <<< "$scenario"

    echo ""
    echo ""
    echo ">>> Running scenario: $name"
    echo ">>> (Users: $users, Spawn rate: $spawn, Candidates: $cand_count, Distributed: $dist)"
    echo ""

    if [ "$dist" == "yes" ]; then
        cmd="benchmark-dist"
    else
        cmd="benchmark"
    fi

    echo ""
    echo "Executing: pixi run $cmd -u $users -r $spawn -t $TIME --match-mode $mode --num-jobs $NUM_JOBS --num-nodes $NUM_NODES --candidate-jobs-count $cand_count"
    echo ""
    echo ""

    pixi run "$cmd" \
        -u "$users" \
        -r "$spawn" \
        -t "$TIME" \
        --match-mode "$mode" \
        --num-jobs "$NUM_JOBS" \
        --num-nodes "$NUM_NODES" \
        --candidate-jobs-count "$cand_count" \
        --log-level ERROR

    echo ""
    echo ""
    echo "------------------------------------------------------------"
    echo ">>> Finished scenario: $name"
    echo "------------------------------------------------------------"
    sleep 5
done

echo "All benchmarks completed successfully."
