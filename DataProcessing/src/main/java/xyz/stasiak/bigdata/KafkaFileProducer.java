package xyz.stasiak.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class KafkaFileProducer {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: KafkaFileProducer <inputDir> <topicName> <bootstrapServers>");
            System.exit(1);
        }
        String inputDir = args[0];
        String topicName = args[1];
        String bootstrapServers = args[2];
        int sleepTime;
        try {
            sleepTime = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            sleepTime = 1000;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        File folder = new File(inputDir);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            System.out.println("No files in the directory " + inputDir);
            System.exit(2);
        }
        String[] listOfPaths = Arrays.stream(listOfFiles)
                .map(File::getAbsolutePath)
                .sorted()
                .toArray(String[]::new);
        for (final String fileName : listOfPaths) {
            try (Stream<String> stream = Files.lines(Paths.get(fileName)).skip(1)) {
                stream.forEach(line -> producer.send(parseLine(topicName, line)));
                System.out.println("File " + fileName + " sent to Kafka");
                TimeUnit.SECONDS.sleep(sleepTime);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static ProducerRecord<String, String> parseLine(String topicName, String line) {
        String key = line.substring(0, line.indexOf(","));
        return new ProducerRecord<>(topicName, key, line);
    }
}
