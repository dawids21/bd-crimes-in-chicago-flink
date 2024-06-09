#!/bin/bash
source ./vars.sh
/usr/lib/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$ANOMALY_OUTPUT_TOPIC" \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.value=true \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer