package org.example.operators;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.example.models.Person;
import org.example.models.PersonalFamily;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class AggregateFamilies extends RichFlatMapFunction<PersonalFamily, Person> {

    private transient ValueState<Map<String, PersonalFamily>> personalFamilyAggregate;


    @Override
    public void flatMap(PersonalFamily personalFamily, Collector<Person> collector) throws Exception {
        Map<String, PersonalFamily> familyMap = personalFamilyAggregate.value();
        if (familyMap == null)
            familyMap = new HashMap<>();
        familyMap.merge(
                personalFamily.familyId(),
                personalFamily,
                (fam1, fam2) -> {
                    PersonalFamily newerChange = fam1.offset() > fam2.offset() ? fam1 : fam2;
                    return Objects.isNull(newerChange.familyMemberRefs()) ? null : newerChange;
                });
        collector.collect(new Person(
                personalFamily.personId(),
                familyMap.values().stream()
                        .flatMap(names -> names.familyMemberRefs().stream())
                        .collect(Collectors.toSet())
                )

        );
        personalFamilyAggregate.update(familyMap);
    }

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<Map<String, PersonalFamily>> descriptor =
                new ValueStateDescriptor<>(
                        "personal-family-aggregate", // the state name
                        TypeInformation.of(new TypeHint<>() {})); // type information

        personalFamilyAggregate = getRuntimeContext().getState(descriptor);
    }
}
