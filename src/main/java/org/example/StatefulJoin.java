package org.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.function.BiFunction;

public class StatefulJoin<T extends Serializable> extends KeyedCoProcessFunction<String, KafkaRecord<T>, KafkaRecord<T>, KafkaRecord<T>> implements Serializable {
    private transient ValueState<KafkaRecord<T>> lastRecord1;
    private transient ValueState<KafkaRecord<T>> lastRecord2;
    private final BiFunction<KafkaRecord<T>, KafkaRecord<T>, KafkaRecord<T>> join;

    public StatefulJoin(BiFunction<KafkaRecord<T>, KafkaRecord<T>, KafkaRecord<T>> join) {
        this.join = join;
    }

    @Override
    public void processElement1(
            KafkaRecord<T> value1,
            Context ctx,
            Collector<KafkaRecord<T>> out) throws Exception {
        KafkaRecord<T> other = lastRecord2.value();
        if (other != null) {
            out.collect(join.apply(value1, other));
        }
        lastRecord1.update(value1);
    }

    @Override
    public void processElement2(
            KafkaRecord<T> value2,
            Context ctx,
            Collector<KafkaRecord<T>> out) throws Exception {
        KafkaRecord<T> other = lastRecord1.value();
        if (other != null) {
            out.collect(join.apply(value2, other));
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
