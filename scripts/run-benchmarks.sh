#!/bin/bash

set -euo pipefail

# Configuration
WARMUP="${WARMUP:-3}"
ITERATIONS="${ITERATIONS:-8}"
FORK="${FORK:-2}"
THREADS="${THREADS:-1}"
INCLUDES="${INCLUDES:-}"
PROFILE_MODE="${PROFILE_MODE:-none}" # none | gc | heap | threads | cpi
RESULT_FORMAT="${RESULT_FORMAT:-TEXT}"
LOG_FILE="benchmarks_execution.log"

OS_NAME="$(uname -s)"
PROFILERS=""

case "$PROFILE_MODE" in
  none)
    ;;
  gc)
    PROFILERS="gc"
    ;;
  heap)
    # heap-oriented signal: allocation + GC counters + HotSpot GC internals
    PROFILERS="gc,hs_gc"
    ;;
  threads)
    # thread/runtime-oriented signal from HotSpot
    PROFILERS="hs_thr,hs_rt"
    ;;
  cpi)
    if [ "$OS_NAME" = "Linux" ]; then
      # perfnorm reports normalized HW counters including CPI (cycles/instruction)
      PROFILERS="perfnorm"
    else
      echo "WARN: PROFILE_MODE=cpi requires Linux perf events. Falling back to gc profiler on $OS_NAME."
      PROFILERS="gc"
    fi
    ;;
  *)
    echo "ERROR: Unsupported PROFILE_MODE='$PROFILE_MODE'."
    echo "Supported modes: none, gc, heap, threads, cpi"
    exit 1
    ;;
esac

echo "Starting KPipe Benchmarks..."
echo "Results will be saved to $LOG_FILE"
echo "Run config: warmup=$WARMUP iterations=$ITERATIONS fork=$FORK threads=$THREADS includes=${INCLUDES:-<all>} profile=$PROFILE_MODE profilers=${PROFILERS:-<none>} resultFormat=$RESULT_FORMAT"

# Clean and run benchmarks from repository root
GRADLE_CMD=(./gradlew :benchmarks:clean :benchmarks:jmh \
  -Pjmh.warmupIterations="$WARMUP" \
  -Pjmh.iterations="$ITERATIONS" \
  -Pjmh.fork="$FORK" \
  -Pjmh.threads="$THREADS" \
  -Pjmh.resultFormat="$RESULT_FORMAT")

if [ -n "$INCLUDES" ]; then
  GRADLE_CMD+=("-Pjmh.includes=$INCLUDES")
fi

if [ -n "$PROFILERS" ]; then
  GRADLE_CMD+=("-Pjmh.profilers=$PROFILERS")
fi

"${GRADLE_CMD[@]}" 2>&1 | tee "$LOG_FILE"

echo "--------------------------------------------------"
echo "Benchmark Summary:"

RESULT_EXT="$(echo "$RESULT_FORMAT" | tr '[:upper:]' '[:lower:]')"
CANDIDATES=(
  "benchmarks/build/results/jmh/results.${RESULT_EXT}"
  "benchmarks/build/results/jmh/results.text"
  "benchmarks/build/results/jmh/results.txt"
)

SUMMARY_FILE=""
for file in "${CANDIDATES[@]}"; do
  if [ -f "$file" ]; then
    SUMMARY_FILE="$file"
    break
  fi
done

if [ -n "$SUMMARY_FILE" ]; then
  cat "$SUMMARY_FILE"
else
  echo "Results file not found. Checked: ${CANDIDATES[*]}. Check $LOG_FILE for details."
fi

echo "Profiler outputs (if enabled) are emitted next to JMH results under benchmarks/build/results/jmh/."
echo "--------------------------------------------------"
