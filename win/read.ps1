$brokerList = "localhost:9092"
$topic = $args[0]

docker exec -it kafka kafka-console-consumer.sh --bootstrap-server $brokerList --topic $topic --from-beginning