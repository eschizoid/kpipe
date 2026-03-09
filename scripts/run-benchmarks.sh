#!/bin/bash

set -euo pipefail

# Configuration
WARMUP="${WARMUP:-3}"
ITERATIONS="${ITERATIONS:-8}"
FORK="${FORK:-2}"
THREADS="${THREADS:-1}"
INCLUDES="${INCLUDES:-}"
LOG_FILE="benchmarks_execution.log"

echo "Starting KPipe Benchmarks..."
echo "Results will be saved to $LOG_FILE"
echo "Run config: warmup=$WARMUP iterations=$ITERATIONS fork=$FORK threads=$THREADS includes=${INCLUDES:-<all>}"

# Create stable temp directory for JMH
mkdir -p benchmarks/build/tmp/jmh &&
cd ..

# Clean and run all benchmarks
GRADLE_CMD=(./gradlew :benchmarks:clean :benchmarks:jmh \
  -Pjmh.warmupIterations=$WARMUP \
  -Pjmh.iterations=$ITERATIONS \
  -Pjmh.fork=$FORK \
  -Pjmh.threads=$THREADS)

if [ -n "$INCLUDES" ]; then
  GRADLE_CMD+=("-Pjmh.includes=$INCLUDES")
fi

"${GRADLE_CMD[@]}" 2>&1 | tee "$LOG_FILE"

echo "--------------------------------------------------"
echo "Benchmark Summary:"
if [ -f "benchmarks/build/results/jmh/results.txt" ]; then
    cat benchmarks/build/results/jmh/results.txt
else
    echo "Results file not found. Check $LOG_FILE for details."
fi
echo "--------------------------------------------------"
