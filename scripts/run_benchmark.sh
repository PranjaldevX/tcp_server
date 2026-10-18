#!/usr/bin/env bash
# Runs the same load against the baseline server and the epoll server, several times each,
# and writes one CSV line per run (SRS PR-2, PR-4, PR-5).
#
# usage: scripts/run_benchmark.sh [runs] [connections] [rate] [payload_bytes] [duration_s]
#   runs           how many times each server is measured          (default 5)
#   connections    concurrent client connections                   (default 100)
#   rate           total requests per second, open loop            (default 20000)
#   payload_bytes  payload size of each request                    (default 64)
#   duration_s     measured seconds per run, after a 2 s warm-up   (default 10)
#
# Edit the SERVERS list below to compare other settings (more workers, --writev, ...).
# Afterwards:  python3 scripts/summarize_results.py benchmark_results/<file>.csv
set -euo pipefail
cd "$(dirname "$0")/.."

RUNS=${1:-5}
CONNECTIONS=${2:-100}
RATE=${3:-20000}
PAYLOAD=${4:-64}
DURATION=${5:-10}
PORT=${PORT:-9300}
LOAD_THREADS=${LOAD_THREADS:-1}

# name|command (without --port, the script adds it)
SERVERS=(
    "baseline|build/baseline_server"
    "epoll_1_worker|build/epoll_server --workers 1"
    "epoll_2_workers|build/epoll_server --workers 2"
)

for binary in build/baseline_server build/epoll_server build/load_generator; do
    [ -x "$binary" ] || { echo "missing $binary, run 'make' first" >&2; exit 1; }
done
ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

mkdir -p benchmark_results
stamp=$(date +%Y%m%d_%H%M%S)
results="benchmark_results/results_${stamp}.csv"
scripts/collect_environment.sh | tee "benchmark_results/environment_${stamp}.txt"
echo
echo "server,run,$(build/load_generator --csv-header)" > "$results"

wait_for_port() {
    for _ in $(seq 1 100); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    return 1
}

for run in $(seq 1 "$RUNS"); do
    for entry in "${SERVERS[@]}"; do
        name=${entry%%|*}
        command=${entry#*|}

        $command --port "$PORT" --stats-interval 0 >/dev/null 2>&1 &
        server_pid=$!
        if ! wait_for_port; then
            echo "server '$name' did not start" >&2
            kill "$server_pid" 2>/dev/null || true
            exit 1
        fi

        echo "run $run/$RUNS  $name"
        line=$(build/load_generator --port "$PORT" --connections "$CONNECTIONS" --threads "$LOAD_THREADS" \
                   --rate "$RATE" --payload-bytes "$PAYLOAD" --duration "$DURATION" --csv 2>/dev/null) || true
        echo "$name,$run,$line" >> "$results"

        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
        sleep 0.5
    done
done

echo
echo "raw results:  $results"
echo "summary:      python3 scripts/summarize_results.py $results"
