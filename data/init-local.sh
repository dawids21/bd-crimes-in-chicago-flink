#!/bin/bash

echo "Checking if kafka topics already exist"
kafka-topics.sh --delete --bootstrap-server localhost:29092 --topic crimes-input || true
kafka-topics.sh --delete --bootstrap-server localhost:29092 --topic iucr-input || true
kafka-topics.sh --delete --bootstrap-server localhost:29092 --topic anomaly-output || true

echo "Creating kafka topics"
kafka-topics.sh --create --topic crimes-input --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic iucr-input --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic anomaly-output --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1

echo "Sending iucr codes data to kafka topic"
cat /data/iucr-small.csv | awk -F ',' 'NR>1{print $1 ":" $1 "," $2 "," $3 "," $4}' | kafka-console-producer.sh --bootstrap-server localhost:29092 --topic iucr-input --property key.separator=: --property parse.key=true
echo "Done"
