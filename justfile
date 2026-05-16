_default:
    @just --list

# Build all modules
build:
    ./gradlew clean spotlessApply build

# Build the docker image (defaults to APP=demo)
docker-build app="demo":
    APP={{app}} docker compose build --no-cache

# Bring the full stack up (kafka, schema registry, observability, app, seeds)
up app="demo":
    APP={{app}} docker compose up -d

# Tear down the stack and wipe volumes
down:
    docker compose down -v

# Restart Grafana (picks up new dashboard JSON)
restart-grafana:
    docker compose restart grafana

# Tail app logs
logs:
    docker compose logs -f app

# Send one batch of JSON messages
seed-json:
    docker compose run --rm --no-deps seed-json

# Send one batch of Avro messages
seed-avro:
    docker compose run --rm --no-deps seed-avro

# Send one batch of Protobuf messages
seed-proto:
    docker compose run --rm --no-deps seed-proto

# Send one batch of all three formats
seed: seed-json seed-avro seed-proto

# Continuously seed all three formats every 5s (Ctrl-C to stop)
seed-loop:
    while true; do just seed; sleep 5; done

# Open Grafana in the browser
grafana:
    open http://localhost:3000/d/kpipe-overview

# Trigger the Release workflow on GitHub Actions (releaseType: patch / minor / major)
release type="minor":
    gh workflow run release.yaml --ref main -f releaseType={{type}}

# Run the parallel-consumer bench (mode=full|smoke|latency, profilers=gc by default)
bench mode="full" profilers="gc":
    #!/usr/bin/env bash
    set -euo pipefail
    rm -f benchmarks/build/tmp/jmh/jmh.lock
    ./gradlew :benchmarks:jmhJar
    JAR=$(find benchmarks/build/libs -name '*-jmh.jar' | head -1)
    OUT=benchmarks/results/$(date +%Y-%m-%d)
    PROF=""
    if [ -n "{{profilers}}" ]; then PROF="-prof {{profilers}}"; fi
    mkdir -p benchmarks/results
    case "{{mode}}" in
      smoke)
        java -jar "$JAR" 'ParallelProcessingBenchmark.kpipe' \
          -p workMicros=0 -wi 1 -i 1 -f 1 \
          -rf JSON -rff "$OUT-smoke.json" | tee "$OUT-smoke.log"
        ;;
      latency)
        java -jar "$JAR" 'ParallelProcessingLatencyBenchmark' \
          -wi 3 -i 5 -f 2 $PROF \
          -rf JSON -rff "$OUT-latency.json" | tee "$OUT-latency.log"
        ;;
      full|*)
        java -jar "$JAR" 'ParallelProcessingBenchmark' \
          -wi 3 -i 5 -f 2 $PROF \
          -rf JSON -rff "$OUT.json" | tee "$OUT.log"
        ;;
    esac
