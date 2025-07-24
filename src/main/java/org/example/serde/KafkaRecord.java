package org.example.serde;

public record KafkaRecord<T> (String key, T value) {
}
