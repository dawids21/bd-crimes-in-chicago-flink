package com.example.bigdata;

public interface Parameters {
    String BOOTSTRAP_SERVERS = "bootstrap-servers";
    String CRIMES_INPUT_TOPIC = "crimes-input-topic";
    String IUCR_INPUT_FILE = "iucr-input-file";
    String ANOMALY_OUTPUT_TOPIC = "anomaly-output-topic";
    String KAFKA_GROUP_ID = "kafka-group-id";
    String FLINK_DELAY = "flink-delay";
    String FLINK_ANOMALY_PERIOD = "flink-anomaly-period";
    String FLINK_ANOMALY_THRESHOLD = "flink-anomaly-threshold";
    String CASSANDRA_HOST = "cassandra-host";
    String CASSANDRA_PORT = "cassandra-port";
    String FLINK_CHECKPOINT_DIR = "flink-checkpoint-dir";
}
