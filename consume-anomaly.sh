#!/bin/bash
source ./vars.sh
/usr/lib/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$ANOMALY_OUTPUT_TOPIC" \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer