package ch.elca.flinksample.models;

import ch.elca.flinksample.serde.KafkaRecord;

import java.util.List;
import java.util.Set;

public record Person (String id, List<Set<String>> familyStates) {
    public KafkaRecord<Person> toKafkaRecord() {
        return KafkaRecord.of(this, this.id());
    }
}
