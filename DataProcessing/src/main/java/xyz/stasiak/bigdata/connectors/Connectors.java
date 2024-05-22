package xyz.stasiak.bigdata.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import xyz.stasiak.bigdata.model.Crime;
import xyz.stasiak.bigdata.model.IucrCode;

public class Connectors {
    public static KafkaSource<Crime> getCrimesSource(ParameterTool properties) {
        return KafkaSource.<Crime>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("crimes-input")
                .setGroupId("flink-chicago-crimes")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new CrimeKafkaDeserializationSchema())
                .build();
    }

    public static KafkaSource<IucrCode> getIucrSource(ParameterTool properties) {
        return KafkaSource.<IucrCode>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("iucr-input")
                .setGroupId("flink-chicago-crimes")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new IucrCodeKafkaDeserializationSchema())
                .build();
    }

    public static KafkaSink<String> getAggSink(ParameterTool properties) {
        return KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("agg-output")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static KafkaSink<String> getAnomalySink(ParameterTool properties) {
        return KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("anomaly-output")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
