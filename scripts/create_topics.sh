#!/usr/bin/env bash
#
# Create the Heimdall Kafka topics on the local broker started by
# docker-compose. Idempotent - re-running prints a notice but does not fail.
#
# Topics:
#   heimdall.raw.business    raw envelopes from the ingestion service
#   heimdall.dlq.ingest      producer-side dead letter (rejected by serializer
#                            or schema before publish)

set -euo pipefail

CONTAINER=${HEIMDALL_KAFKA_CONTAINER:-heimdall-kafka}
BOOTSTRAP=${HEIMDALL_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "Kafka container '${CONTAINER}' is not running. Start it with 'make kafka-up'." >&2
  exit 1
fi

create_topic () {
  local name=$1
  local partitions=$2
  local rf=$3
  local extra_cfg=$4

  echo "create-or-skip ${name} (partitions=${partitions} rf=${rf})"
  docker exec "${CONTAINER}" \
    kafka-topics.sh \
      --bootstrap-server "${BOOTSTRAP}" \
      --create \
      --if-not-exists \
      --topic "${name}" \
      --partitions "${partitions}" \
      --replication-factor "${rf}" \
      ${extra_cfg}
}

create_topic "heimdall.raw.business" 6 1 "--config retention.ms=604800000 --config compression.type=producer"
create_topic "heimdall.dlq.ingest"   3 1 "--config retention.ms=2592000000"

echo
echo "Topics:"
docker exec "${CONTAINER}" \
  kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list \
  | grep -E '^heimdall\.' || true
