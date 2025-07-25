package org.example.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

@Slf4j
public class KafkaSerDe {

    private KafkaSerDe() {}

    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static <T> KafkaRecordDeserializationSchema<KafkaRecord<T>> getDeserializer(Class<T> clazz) {
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
                            : KafkaSerDe.objectMapper.readValue(valueString, clazz);
                    collector.collect(new KafkaRecord<>(key, consumerRecord.offset(), value));
                } catch (JsonProcessingException e) {
                    log.error(e.getMessage());
                }
            }

            @Override
            public TypeInformation<KafkaRecord<T>> getProducedType() {
                return TypeInformation.of(new TypeHint<>() {
                });
            }
        };
    }

    public static <T> KafkaRecordSerializationSchema<KafkaRecord<T>> getSerializer(String topic) {
        return (element, ctx, timestamp) ->
        {
            try {
                return new ProducerRecord<>(
                        topic,
                        /* partition */ null,
                        /* timestamp */ System.currentTimeMillis(),
                        /* key */ element.key().getBytes(StandardCharsets.UTF_8),
                        /* value */ KafkaSerDe.objectMapper.writeValueAsString(element.value()).getBytes(StandardCharsets.UTF_8)
                );
            } catch (JsonProcessingException e) {
                log.error(e.getMessage());
                return null;
            }
        };
    }
}


