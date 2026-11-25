#!/bin/sh
# Idempotent topic setup. Partitioned by symbol key; 6 partitions lets up to 6 processors share load.
set -e
B="${KAFKA_BOOTSTRAP:-kafka:9092}"
T=/opt/kafka/bin/kafka-topics.sh
for topic in market.raw market.validated market.processed; do
  $T --bootstrap-server "$B" --create --if-not-exists --topic "$topic" --partitions 6 --replication-factor 1 \
     --config retention.ms=86400000
done
# Errors are kept longer for investigation.
$T --bootstrap-server "$B" --create --if-not-exists --topic market.errors --partitions 3 --replication-factor 1 \
   --config retention.ms=604800000
$T --bootstrap-server "$B" --list
