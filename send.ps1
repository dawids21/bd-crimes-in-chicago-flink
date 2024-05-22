$brokerList = "localhost:9092"
$topic = "crimes-input"
$message = $args[0]

echo $message | docker exec -i kafka kafka-console-producer.sh --bootstrap-server $brokerList --topic $topic --property parse.key=true --property key.separator=:
