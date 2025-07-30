package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.serde.KafkaRecord;
import org.example.serde.KafkaSerDe;

import java.time.Duration;

public class JoinExample {

    static final String BOOTSTRAP_SERVERS = "kafka:19092";

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KafkaRecord<String>> source = KafkaSource.<KafkaRecord<String>>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaSerDe.getStringDeserializer())
                .build();

        KafkaSource<KafkaRecord<String>> additionalSource = KafkaSource.<KafkaRecord<String>>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("input-topic2")
                .setGroupId("my-group2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaSerDe.getStringDeserializer())
                .build();

        KafkaSink<KafkaRecord<String>> sink = KafkaSink.<KafkaRecord<String>>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaSerDe.getSerializer("output-topic"))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        WatermarkStrategy<KafkaRecord<String>> strategy = WatermarkStrategy
                .<KafkaRecord<String>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis());

        DataStream<KafkaRecord<String>> inputs = env.fromSource(source, strategy, "Kafka Source");

        DataStream<KafkaRecord<String>> inputs2 = env.fromSource(additionalSource, strategy, "Kafka Source");

        inputs
                .keyBy(KafkaRecord::key)
                .connect(inputs2.keyBy(KafkaRecord::key))
                .process(new org.example.operators.StatefulJoin<>((r1, r2) -> new KafkaRecord<>(
                        r1.key(),
                        System.currentTimeMillis(),
                        r1.value() + " " + r2.value()
                )))
                .sinkTo(sink);

        inputs2
                .keyBy(KafkaRecord::key)
                .connect(inputs.keyBy(KafkaRecord::key))
                .process(new org.example.operators.StatefulJoin<>((r1, r2) -> new KafkaRecord<>(
                        r1.key(),
                        System.currentTimeMillis(),
                        "! " + r1.value() + " " + r2.value() + " !"
                )))
                .sinkTo(sink);

        env.execute();
    }

}