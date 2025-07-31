package ch.elca.flinksample.models;

import ch.elca.flinksample.serde.KafkaRecord;

import java.util.Set;

public record Person (String id, Set<String> familyMemberRefs) {
    public KafkaRecord<Person> toKafkaRecord() {
        return KafkaRecord.of(this, this.id());
    }
}
