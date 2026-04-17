#!/bin/bash
set -eu pipefail

SCHEMA_FILE="${1:-/schemas/customer.avsc}"
SUBJECT="${2:-com.kpipe.customer}"
REGISTRY_URL="${3:-http://schema-registry:8081}"

until curl -fsS "${REGISTRY_URL}/subjects" >/dev/null; do
  echo "Waiting for Schema Registry at ${REGISTRY_URL}..."
  sleep 2
done

if curl -fsS "${REGISTRY_URL}/subjects/${SUBJECT}/versions/latest" >/dev/null 2>&1; then
  echo "Schema subject ${SUBJECT} already exists, skipping."
  exit 0
fi

PAYLOAD="$(jq -Rs '{schema: .}' "${SCHEMA_FILE}")"

curl -fsS -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "${PAYLOAD}" \
  "${REGISTRY_URL}/subjects/${SUBJECT}/versions" >/dev/null

echo "Registered schema for subject ${SUBJECT}"
