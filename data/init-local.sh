#!/bin/bash

echo "Checking if kafka topics already exist"
kafka-topics.sh --delete --bootstrap-server localhost:29092 --topic crimes-input || true
kafka-topics.sh --delete --bootstrap-server localhost:29092 --topic anomaly-output || true

echo "Creating kafka topics"
kafka-topics.sh --create --topic crimes-input --bootstrap-server localhost:29092 --replication-factor 1 --partitions 2
kafka-topics.sh --create --topic anomaly-output --bootstrap-server localhost:29092 --replication-factor 1 --partitions 2

echo "Done"
