package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class Serde {

    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static <T extends Serializable> KafkaRecordDeserializationSchema<KafkaRecord<T>> getDeserializer(Class<T> clazz) {
        return new KafkaRecordDeserializationSchema<>() {

            @Override
            public void open(DeserializationSchema.InitializationContext context) throws Exception {
                KafkaRecordDeserializationSchema.super.open(context);
            }

            @Override
            public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaRecord<T>> collector) {
                try {
                    String key = consumerRecord.key() == null
                            ? null
                            : new String(consumerRecord.key(), StandardCharsets.UTF_8);
                    String valueString = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                    T value = clazz.equals(String.class)
                            ? (T) valueString
                            : Serde.objectMapper.readValue(valueString, clazz);
                    collector.collect(new KafkaRecord<>(key, value));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public TypeInformation<KafkaRecord<T>> getProducedType() {
                return TypeInformation.of(new TypeHint<>() {
                });
            }
        };
    }

    public static <T extends Serializable> KafkaRecordSerializationSchema<KafkaRecord<T>> getSerializer() {
        return (element, ctx, timestamp) ->
        {
            try {
                return new ProducerRecord<>(
                        "output-topic",
                        /* partition */ null,
                        /* timestamp */ System.currentTimeMillis(),
                        /* key */ element.key().getBytes(StandardCharsets.UTF_8),
                        /* value */ Serde.objectMapper.writeValueAsString(element.value()).getBytes(StandardCharsets.UTF_8)
                );
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }
}


