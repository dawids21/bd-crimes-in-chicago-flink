#!/bin/bash
source ./vars.sh

echo "Killing yarn applications"
applications=$(yarn application --list | awk '{print $1}' | grep application)
if [ -z "$applications" ]; then
    echo "No yarn applications to kill"
else
    yarn application --list | awk '{print $1}' | grep application | xargs yarn application -kill
fi

echo "Cleaning up"
rm -rf "$INPUT_DIR"
rm -rf "$INPUT_DIR.zip"
rm "$IUCR_INPUT_FILE"
hadoop fs -rm -r -f "$FLINK_CHECKPOINT_DIR"

echo "Copying input files from GCS"
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv "$IUCR_INPUT_FILE" || exit
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/crimes-in-chicago_result.zip "$INPUT_DIR.zip" || exit

echo "Unzipping input files"
unzip -j "$INPUT_DIR.zip" -d "$INPUT_DIR" || exit

echo "Downloading dependencies"
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.15.4/flink-connector-kafka-1.15.4.jar
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/1.15.4/flink-connector-cassandra_2.12-1.15.4.jar
sudo cp ~/*-*.jar /usr/lib/flink/lib/

echo "Checking if kafka topics already exist"
kafka-topics.sh --delete --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$CRIMES_INPUT_TOPIC"
kafka-topics.sh --delete --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$ANOMALY_OUTPUT_TOPIC"

echo "Creating kafka topics"
kafka-topics.sh --create --topic "$CRIMES_INPUT_TOPIC" --bootstrap-server "$BOOTSTRAP_SERVERS" --replication-factor 1 --partitions 2
kafka-topics.sh --create --topic "$ANOMALY_OUTPUT_TOPIC" --bootstrap-server "$BOOTSTRAP_SERVERS" --replication-factor 1 --partitions 1

echo "Starting cassandra"
sudo apt-get update
sudo apt-get install docker-compose-plugin
docker compose down
docker compose up -d --wait

echo "Preparing cassandra schema"
docker exec -it cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS crime_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
                                    USE crime_data;
                                    CREATE TABLE IF NOT EXISTS crime_aggregate
                                    (
                                        district               INT,
                                        month                  INT,
                                        primary_description    TEXT,
                                        count                  BIGINT,
                                        count_arrest           BIGINT,
                                        count_domestic         BIGINT,
                                        count_monitored_by_fbi BIGINT,
                                        PRIMARY KEY ((district), month, primary_description)
                                    );
                                    TRUNCATE crime_data.crime_aggregate;"

echo "Reset complete"