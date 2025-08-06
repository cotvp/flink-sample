package ch.elca.flinksample.operators;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ch.elca.flinksample.models.Person;
import ch.elca.flinksample.models.PersonalFamilyState;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AggregateFamilies extends RichMapFunction<PersonalFamilyState, Person> {

    private transient ValueState<Map<String, PersonalFamilyState>> personalFamilyAggregate;

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<Map<String, PersonalFamilyState>> descriptor =
                new ValueStateDescriptor<>(
                        "personal-family-aggregate", // the state name
                        TypeInformation.of(new TypeHint<>() {})); // type information

        personalFamilyAggregate = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Person map(PersonalFamilyState personalFamilyState) throws Exception {
        Map<String, PersonalFamilyState> familyMap = personalFamilyAggregate.value();
        if (familyMap == null)
            familyMap = new HashMap<>();
        familyMap.merge(
                personalFamilyState.familyId(),
                personalFamilyState,
                (fam1, fam2) -> {
                    PersonalFamilyState newerChange = fam1.offset() > fam2.offset() ? fam1 : fam2;
                    return Objects.isNull(newerChange.familyMemberRefs()) ? null : newerChange;
                });
        personalFamilyAggregate.update(familyMap);
        return new Person(
                personalFamilyState.personId(),
                familyMap.values().stream()
                        .map(PersonalFamilyState::familyMemberRefs)
                        .toList()
        );
    }
}
