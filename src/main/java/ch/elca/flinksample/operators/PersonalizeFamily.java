package ch.elca.flinksample.operators;

import ch.elca.flinksample.serde.KafkaRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import ch.elca.flinksample.models.FamilyChange;
import ch.elca.flinksample.models.PersonalFamilyState;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class PersonalizeFamily extends RichFlatMapFunction<KafkaRecord<FamilyChange>, PersonalFamilyState> {

    private transient ValueState<FamilyChange> previousFamilyState;
    @Override
    public void flatMap(KafkaRecord<FamilyChange> family, Collector<PersonalFamilyState> collector) throws Exception {
        // get last state of the family from state store
        Set<String> previousFamilyMembers = new HashSet<>(Optional.ofNullable(previousFamilyState.value())
                .map(FamilyChange::familyMemberRefs)
                .orElse(Collections.emptySet()));
        // get familyMemberRefs that are not part of the family anymore
        previousFamilyMembers.removeAll(family.value().familyMemberRefs());
        // create personal "tombstone" families
        previousFamilyMembers.forEach(personRef ->
                collector.collect(new PersonalFamilyState(personRef, family.key(), family.timestamp(), null))
        );
        // create current personal families
        family.value().familyMemberRefs().forEach(personRef ->
                collector.collect(new PersonalFamilyState(personRef, family.key(), family.timestamp(), family.value().familyMemberRefs()))
        );
        previousFamilyState.update(family.value());
    }

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<FamilyChange> descriptor =
                new ValueStateDescriptor<>(
                        "previous-family", // the state name
                        TypeInformation.of(new TypeHint<>() {})); // type information

        previousFamilyState = getRuntimeContext().getState(descriptor);
    }
}
