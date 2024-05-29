package xyz.stasiak.bigdata.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import xyz.stasiak.bigdata.Parameters;
import xyz.stasiak.bigdata.model.Crime;
import xyz.stasiak.bigdata.model.CrimeAggregate;
import xyz.stasiak.bigdata.model.IucrCode;

public class Connectors {
    public static KafkaSource<Crime> getCrimesSource(ParameterTool properties) {
        return KafkaSource.<Crime>builder()
                .setBootstrapServers(properties.get(Parameters.BOOTSTRAP_SERVERS))
                .setTopics(properties.get(Parameters.CRIMES_INPUT_TOPIC))
                .setGroupId(properties.get(Parameters.KAFKA_GROUP_ID))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new CrimeKafkaDeserializationSchema())
                .build();
    }

    public static KafkaSource<IucrCode> getIucrSource(ParameterTool properties) {
        return KafkaSource.<IucrCode>builder()
                .setBootstrapServers(properties.get(Parameters.BOOTSTRAP_SERVERS))
                .setTopics(properties.get(Parameters.IUCR_INPUT_TOPIC))
                .setGroupId(properties.get(Parameters.KAFKA_GROUP_ID))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new IucrCodeKafkaDeserializationSchema())
                .build();
    }

    public static CassandraSink<CrimeAggregate> getCassandraAggSink(DataStream<CrimeAggregate> input, ParameterTool properties) throws Exception {
        return CassandraSink.addSink(input)
                .setHost(properties.get(Parameters.CASSANDRA_HOST), properties.getInt(Parameters.CASSANDRA_PORT, 9042))
                .build();
    }

    public static KafkaSink<String> getAnomalySink(ParameterTool properties) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(properties.get(Parameters.BOOTSTRAP_SERVERS))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(properties.get(Parameters.ANOMALY_OUTPUT_TOPIC))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
