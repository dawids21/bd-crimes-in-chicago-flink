docker exec -it kafka bash -c "/data/init-local.sh"
Write-Output "Producing crimes data"
java -cp .\FlinkCrimesChicago\target\FlinkCrimesChicago.jar com.example.bigdata.KafkaFileProducer .\data\crimes crimes-input localhost:29092 2
Write-Output "Removing existing results"
docker exec -it cassandra cqlsh -e "TRUNCATE crime_data.crime_aggregate;"