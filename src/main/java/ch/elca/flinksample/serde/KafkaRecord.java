package ch.elca.flinksample.serde;

public record KafkaRecord<T> (String key, Long timestamp, T value) {
    public static <T> KafkaRecord<T> of (T object, String key) {
        return new KafkaRecord<>(key, null, object);
    }
}
