package ch.elca.flinksample.operators;

import ch.elca.flinksample.serde.KafkaRecord;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


class StatefulJoinTest {
    private KeyedTwoInputStreamOperatorTestHarness<String, KafkaRecord<String>, KafkaRecord<String>, KafkaRecord<String>> testHarness;

    @BeforeEach
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        StatefulJoin<String> statefulFlatMapFunction = new StatefulJoin<>((record1, record2) ->
                new KafkaRecord<>(record1.key(), null, record1.value() + " | " + record2.value()));

        // wrap user defined function into the corresponding operator
        testHarness = new KeyedTwoInputStreamOperatorTestHarness <>(
                new KeyedCoProcessOperator<>(statefulFlatMapFunction),
                KafkaRecord::key,
                KafkaRecord::key,
                BasicTypeInfo.STRING_TYPE_INFO
        );

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    void testingStatefulFlatMapFunction() throws Exception {

        KafkaRecord<String> Topic1KeyA_1 = new KafkaRecord<>("a", null, "1: first a");
        KafkaRecord<String> Topic2KeyB_1 = new KafkaRecord<>("b", null, "2: first b");
        KafkaRecord<String> Topic1KeyA_2 = new KafkaRecord<>("a", null, "1: second a");
        KafkaRecord<String> Topic2KeyA_1 = new KafkaRecord<>("a", null, "2: first a");
        KafkaRecord<String> Topic1KeyB_1 = new KafkaRecord<>("b", null, "1: first b");

        //push (timestamped) elements into the operator (and hence user defined function)
        testHarness.processElement1(Topic1KeyA_1, 100L);
        testHarness.processElement2(Topic2KeyB_1, 200L);
        testHarness.processElement1(Topic1KeyA_2, 300L);
        testHarness.processElement2(Topic2KeyA_1, 400L);
        testHarness.processElement1(Topic1KeyB_1, 500L);

        //trigger event time timers by advancing the event time of the operator with a watermark
        testHarness.processBothWatermarks(Watermark.MAX_WATERMARK);

        //trigger processing time timers by advancing the processing time of the operator directly
        testHarness.setProcessingTime(500L);

        //retrieve list of emitted records for assertions
        assertThat(testHarness.extractOutputValues()).satisfiesExactly(
                record -> assertThat(record).hasFieldOrPropertyWithValue("value", "1: second a | 2: first a"),
                record -> assertThat(record).hasFieldOrPropertyWithValue("value", "1: first b | 2: first b")
        );
    }
}
