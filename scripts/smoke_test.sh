#!/usr/bin/env bash
#
# End-to-end smoke test against the local Kafka stack. Verifies:
#  1. Topics exist
#  2. A one-shot ingest publishes >0 records to heimdall.raw.business
#  3. An injected bad record lands on heimdall.dlq.ingest
#
# This is not a replacement for unit tests; it is a precommit confidence check.

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "${ROOT_DIR}"

if [[ -z "${YELP_API_KEY:-}" ]]; then
  if [[ -f .env ]]; then
    # shellcheck disable=SC1091
    set -a; source .env; set +a
  fi
fi

if [[ -z "${YELP_API_KEY:-}" ]]; then
  echo "YELP_API_KEY is not set (no env, no .env). Aborting." >&2
  exit 2
fi

CONTAINER=${HEIMDALL_KAFKA_CONTAINER:-heimdall-kafka}
TOPIC_RAW=${KAFKA_TOPIC_RAW:-heimdall.raw.business}
TOPIC_DLQ=${KAFKA_TOPIC_DLQ:-heimdall.dlq.ingest}

echo "== verifying topics =="
docker exec "${CONTAINER}" kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "${TOPIC_RAW}"
docker exec "${CONTAINER}" kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "${TOPIC_DLQ}"

echo "== running one-shot ingest =="
.venv/bin/python -m ingest --once --max-cities=1 --max-pages-per-city=1

echo "== counting messages in ${TOPIC_RAW} =="
N=$(docker exec "${CONTAINER}" \
    kafka-run-class.sh kafka.tools.GetOffsetShell \
      --bootstrap-server kafka:9092 --topic "${TOPIC_RAW}" --time -1 \
    | awk -F: '{ s += $3 } END { print s }')
echo "raw topic offset sum: ${N}"
if [[ "${N}" -le 0 ]]; then
  echo "expected >0 messages on ${TOPIC_RAW}" >&2
  exit 3
fi

echo "== injecting an intentionally bad record =="
.venv/bin/python -m ingest --inject-bad

echo "== counting messages in ${TOPIC_DLQ} =="
M=$(docker exec "${CONTAINER}" \
    kafka-run-class.sh kafka.tools.GetOffsetShell \
      --bootstrap-server kafka:9092 --topic "${TOPIC_DLQ}" --time -1 \
    | awk -F: '{ s += $3 } END { print s }')
echo "dlq topic offset sum: ${M}"
if [[ "${M}" -le 0 ]]; then
  echo "expected >0 messages on ${TOPIC_DLQ} after bad-record injection" >&2
  exit 4
fi

echo "smoke test OK"
