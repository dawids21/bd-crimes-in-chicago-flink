#!/bin/bash
source ./vars.sh
flink run -m yarn-cluster -p 2 \
   -yjm 1024m -ytm 1024m -c \
   com.example.bigdata.Main FlinkCrimesChicago.jar \
    --crimes-input-topic "$CRIMES_INPUT_TOPIC" \
    --iucr-input-topic "$IUCR_INPUT_TOPIC" \
    --anomaly-output-topic "$ANOMALY_OUTPUT_TOPIC" \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --kafka-group-id "$KAFKA_GROUP_ID" \
    --cassandra-host "$CASSANDRA_HOST" \
    --cassandra-port "$CASSANDRA_PORT" \
    --flink-delay "$FLINK_DELAY" \
    --flink-anomaly-period "$FLINK_ANOMALY_PERIOD" \
    --flink-anomaly-threshold "$FLINK_ANOMALY_THRESHOLD" \
    --flink-checkpoint-dir "$FLINK_CHECKPOINT_DIR" \
    $@
