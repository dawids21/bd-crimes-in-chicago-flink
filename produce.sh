#!/bin/bash
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar xyz.stasiak.bigdata.KafkaFileProducer "$INPUT_DIR" "$CRIMES_INPUT_TOPIC" "$BOOTSTRAP_SERVERS"
