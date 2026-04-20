#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Building demo fat-jar ==="
"$ROOT_DIR/gradlew" -p "$ROOT_DIR" :examples:demo:shadowJar

echo "=== Starting demo stack ==="
docker compose -f "$ROOT_DIR/examples/demo/docker-compose.yaml" up --build "$@"

