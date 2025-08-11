package ch.elca.flinksample;

import ch.elca.flinksample.models.FamilyChange;
import ch.elca.flinksample.models.Person;
import ch.elca.flinksample.models.PersonalFamilyState;
import ch.elca.flinksample.operators.AggregateFamilies;
import ch.elca.flinksample.operators.PersonalizeFamily;
import ch.elca.flinksample.serde.KafkaRecord;
import ch.elca.flinksample.serde.KafkaSerDe;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FamilyExample {

    static final String BOOTSTRAP_SERVERS = "kafka:19092";

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KafkaRecord<FamilyChange>> source = KafkaSource.<KafkaRecord<FamilyChange>>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("family-topic")
                .setGroupId("family-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaSerDe.getPOJODeserializer(FamilyChange.class))
                .build();


        KafkaSink<KafkaRecord<Person>> sink = KafkaSink.<KafkaRecord<Person>>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaSerDe.getSerializer("family-members"))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();


        DataStream<KafkaRecord<FamilyChange>> inputs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        inputs
                .keyBy(KafkaRecord::key)
                .flatMap(new PersonalizeFamily())
                .keyBy(PersonalFamilyState::personId)
                .map(new AggregateFamilies())
                .map(Person::toKafkaRecord)
                .sinkTo(sink);

        env.execute();
    }

}