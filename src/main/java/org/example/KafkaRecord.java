package org.example;

public record KafkaRecord<T> (String key, T value) {
}
