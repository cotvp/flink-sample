package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class Main {

    static final String BOOTSTRAP_SERVERS = "kafka:19092";

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaRecordDeserializationSchema<KafkaRecord> kvDeser =
                new KafkaRecordDeserializationSchema<>() {
                    @Override
                    public void open(DeserializationSchema.InitializationContext context) throws Exception {
                        KafkaRecordDeserializationSchema.super.open(context);
                    }

                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaRecord> collector) {
                        String key = consumerRecord.key() == null
                                ? null
                                : new String(consumerRecord.key(), StandardCharsets.UTF_8);
                        String value = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                        collector.collect(new KafkaRecord(key, value));
                    }

                    @Override
                    public TypeInformation<KafkaRecord> getProducedType() {
                        return TypeInformation.of(new TypeHint<>() {});
                    }
                };

        KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(kvDeser)
                .build();

        KafkaSource<KafkaRecord> additionalSource = KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("input-topic2")
                .setGroupId("my-group2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(kvDeser)
                .build();

        KafkaRecordSerializationSchema<KafkaRecord> serializer =
                (element, ctx, timestamp) ->
                        new ProducerRecord<>(
                                "output-topic",
                                /* partition */ null,
                                /* timestamp */ System.currentTimeMillis(),
                                /* key */ element.key().getBytes(StandardCharsets.UTF_8),
                                /* value */ element.value().getBytes(StandardCharsets.UTF_8)
                        );

        KafkaSink<KafkaRecord> sink = KafkaSink.<KafkaRecord>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        WatermarkStrategy<KafkaRecord> strategy = WatermarkStrategy
                .<KafkaRecord>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis());

        DataStream<KafkaRecord> inputs = env.fromSource(source, strategy, "Kafka Source");

        DataStream<KafkaRecord> inputs2 = env.fromSource(additionalSource, strategy, "Kafka Source");

        inputs
                .keyBy(KafkaRecord::key)
                .connect(inputs2.keyBy(KafkaRecord::key))
                .process(new StatefulJoin())
                .sinkTo(sink);

        env.execute();
    }

}