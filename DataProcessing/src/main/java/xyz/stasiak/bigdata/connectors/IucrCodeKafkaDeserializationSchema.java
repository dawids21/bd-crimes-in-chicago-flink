package xyz.stasiak.bigdata.connectors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import xyz.stasiak.bigdata.model.IucrCode;

public class IucrCodeKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<IucrCode> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<IucrCode> out) {
        IucrCode iucrCode = new IucrCode();
        iucrCode.setCode(getCode(new String(record.key())));
        String[] values = new String(record.value()).split(",");
        iucrCode.setPrimaryDescription(values[1]);
        iucrCode.setSecondaryDescription(values[2]);
        iucrCode.setMonitoredByFbi("I".equals(values[3]));
        out.collect(iucrCode);
    }

    private String getCode(String key) {
        if (key.length() < 4) {
            key = "0".repeat(4 - key.length()) + key;
        }
        return key;
    }

    @Override
    public TypeInformation<IucrCode> getProducedType() {
        return TypeInformation.of(IucrCode.class);
    }
}
