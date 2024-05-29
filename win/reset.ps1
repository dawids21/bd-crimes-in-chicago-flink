docker exec -it kafka bash -c "/data/init-local.sh"
echo "Producing crimes data"
java -jar .\KafkaProducer\target\KafkaProducer-1.0-SNAPSHOT.jar .\data\crimes crimes-input localhost:29092
echo "Removing existing results"
docker exec -it cassandra cqlsh -e "TRUNCATE crime_data.crime_aggregate;"
# https://github.com/jordyv/wait-for-healthy-container/blob/master/wait-for-healthy-container.sh