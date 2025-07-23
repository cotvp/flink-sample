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
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TaggedUnion;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Main {

    static final String BOOTSTRAP_SERVERS = "kafka:19092";

    public record KafkaRecord (String key, String value) {}
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
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaRecord> collector) throws IOException {
                        String key = consumerRecord.key() == null
                                ? null
                                : new String(consumerRecord.key(), StandardCharsets.UTF_8);
                        String value = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                        collector.collect(new KafkaRecord(key, value));
                    }

                    private static final long serialVersionUID = 1L;


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
                                /* key */ element.key.getBytes(StandardCharsets.UTF_8),
                                /* value */ element.value.getBytes(StandardCharsets.UTF_8)
                        );

        KafkaSink<KafkaRecord> sink = KafkaSink.<KafkaRecord>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        DataStream<KafkaRecord> inputs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<KafkaRecord> inputs2 = env.fromSource(additionalSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        inputs
                .join(inputs2)
                .where(keyedRecord -> keyedRecord.key)
                .equalTo(keyedRecord -> keyedRecord.key)
                .window(GlobalWindows.create())
                .trigger(new Trigger<>() {
                    @Override
                    public TriggerResult onElement(TaggedUnion<KafkaRecord, KafkaRecord> keyedRecordKeyedRecordTaggedUnion, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                        // nothing to clear
                    }
                })
                .apply((kr1, kr2) -> new KafkaRecord(
                                kr1.key,
                                kr1.value + "/" + kr2.value
                        )

                )
                .sinkTo(sink);
        env.execute();
    }

}