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
