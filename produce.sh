#!/bin/bash
source ./vars.sh
java -cp /usr/lib/kafka/libs/*:FlinkCrimesChicago.jar xyz.stasiak.bigdata.KafkaFileProducer "$INPUT_DIR" "$CRIMES_INPUT_TOPIC" "$BOOTSTRAP_SERVERS" "$KAFKA_SLEEP_TIME"
