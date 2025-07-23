package org.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class StatefulJoin extends KeyedCoProcessFunction<String, KafkaRecord, KafkaRecord, KafkaRecord> {
    private transient ValueState<KafkaRecord> lastRecord1;
    private transient ValueState<KafkaRecord> lastRecord2;

    @Override
    public void processElement1(
            KafkaRecord value1,
            Context ctx,
            Collector<KafkaRecord> out) throws Exception {
        KafkaRecord other = lastRecord2.value();
        if (other != null) {
            String joinedValue = value1.value() + "/" + other.value();
            out.collect(new KafkaRecord(value1.key(), joinedValue));
        }
        lastRecord1.update(value1);
    }

    @Override
    public void processElement2(
            KafkaRecord value2,
            Context ctx,
            Collector<KafkaRecord> out) throws Exception {
        KafkaRecord other = lastRecord1.value();
        if (other != null) {
            String joinedValue = other.value() + "/" + value2.value();
            out.collect(new KafkaRecord(value2.key(), joinedValue));
        }
        lastRecord2.update(value2);
    }

    @Override
    public void open(OpenContext openContext) {
        lastRecord1 = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "lastRecord1",
                        TypeInformation.of(new TypeHint<>() {
                        })
                )
        );
        lastRecord2 = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "lastRecord2",
                        TypeInformation.of(new TypeHint<>() {
                        })
                )
        );
    }
}
