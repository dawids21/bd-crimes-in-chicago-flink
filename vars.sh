#!/bin/bash
export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
export BUCKET_NAME="bigdata-23-ds"
export INPUT_DIR="/tmp/crimes-input"
export INPUT_FILE="/tmp/iucr.csv"
export CASSANDRA_HOST="$CLUSTER_NAME-m"
export CASSANDRA_PORT="9042"

export CRIMES_INPUT_TOPIC="crimes-input"
export IUCR_INPUT_TOPIC="iucr-input"
export ANOMALY_OUTPUT_TOPIC="anomaly-output"
export BOOTSTRAP_SERVERS="$CLUSTER_NAME-w-0:9092"
export KAFKA_GROUP_ID="flink-crimes-chicago"

export HADOOP_CONF_DIR="/etc/hadoop/conf"
export HADOOP_CLASSPATH=$(hadoop classpath)

export FLINK_DELAY="A"
export FLINK_ANOMALY_PERIOD=7
export FLINK_ANOMALY_THRESHOLD=0.1