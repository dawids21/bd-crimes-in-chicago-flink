#!/bin/bash
export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
export BUCKET_NAME=""
export INPUT_DIR="/tmp/crimes-input"
export IUCR_INPUT_FILE="/tmp/iucr.csv"
export CASSANDRA_HOST="$CLUSTER_NAME-m"
export CASSANDRA_PORT="9042"

export CRIMES_INPUT_TOPIC="crimes-input"
export ANOMALY_OUTPUT_TOPIC="anomaly-output"
export BOOTSTRAP_SERVERS="$CLUSTER_NAME-w-0:9092"
export KAFKA_GROUP_ID="flink-crimes-chicago"
export KAFKA_SLEEP_TIME=2

export HADOOP_CONF_DIR="/etc/hadoop/conf"
export HADOOP_CLASSPATH=$(hadoop classpath)

export FLINK_CHECKPOINT_DIR="hdfs:///tmp/flink-checkpoints"
export FLINK_DELAY="C"
export FLINK_ANOMALY_PERIOD=30
export FLINK_ANOMALY_THRESHOLD=60