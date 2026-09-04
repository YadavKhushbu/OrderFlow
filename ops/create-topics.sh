#!/bin/sh
# Provisions every OrderFlow topic before any service starts.
#
# Topics are infrastructure. Partition counts are a capacity decision, and
# leaving them to whichever application boots first is how a topic ends up
# auto-created with one partition and then grown to three underneath a consumer
# group that has already been assigned — after which messages hashed to the new
# partitions are simply never consumed.
set -e

BROKER="${BROKER:-kafka:19092}"
TOPICS_SH=/opt/kafka/bin/kafka-topics.sh

create() {
  echo "creating $1 (partitions=$2)"
  "$TOPICS_SH" --bootstrap-server "$BROKER" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

create orderflow.commands.inventory 3
create orderflow.commands.payment   3
create orderflow.events.inventory   3
create orderflow.events.payment     3
create orderflow.events.order       3

# Single partition: dead letters are read by people, and ordering helps them.
create orderflow.dlt 1

echo "topics-provisioned"
