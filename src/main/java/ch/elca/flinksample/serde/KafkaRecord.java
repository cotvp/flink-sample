package ch.elca.flinksample.serde;

public record KafkaRecord<T> (String key, Long offset, T value) {

    public static <T> KafkaRecord<T> of (T object, String key) {
        return new KafkaRecord<>(key, null, object);
    }
}
