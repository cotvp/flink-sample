package org.example.models;

import org.example.serde.KafkaRecord;

import java.util.Set;

public record Person (String id, Set<String> familyMemberRefs) {
    public KafkaRecord<Person> toKafkaRecord() {
        return KafkaRecord.of(this, this.id());
    }
}
