package com.example.bigdata.connectors;

import com.example.bigdata.model.Crime;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CrimeKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<Crime> {

    private DateTimeFormatter formatter;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Crime> out) {
        String[] values = new String(record.value()).split(",");
        if ("\"\"".equals(values[5])) {
            return;
        }
        Crime crime = new Crime();
        crime.setId(Long.parseLong(new String(record.key())));
        crime.setDate(LocalDateTime.parse(values[1], formatter));
        crime.setIucrCode(values[2]);
        crime.setArrest(Boolean.parseBoolean(values[3]));
        crime.setDomestic(Boolean.parseBoolean(values[4]));
        crime.setDistrict(Math.round(Float.parseFloat(values[5])));
        out.collect(crime);
    }

    @Override
    public TypeInformation<Crime> getProducedType() {
        return TypeInformation.of(Crime.class);
    }
}
