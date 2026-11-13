#!/usr/bin/env bash
# Fails when a version pinned by hand has moved. Dependabot opens pull requests against these files,
# so this turns such a pull request red instead of letting it merge.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
# Confluent Parallel Consumer 0.5.3.3 processes no records on kafka-clients 4.4.0; see
# benchmarks/build.gradle.kts.
if ! grep -qF 'listOf("org.apache.kafka", "kafka-clients", "4.3.0")' benchmarks/build.gradle.kts; then
  echo "PIN MOVED: benchmarks/build.gradle.kts must pin kafka-clients 4.3.0 for Confluent Parallel Consumer"
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "check-pins: FAILED"
  exit 1
fi
echo "check-pins: OK"
