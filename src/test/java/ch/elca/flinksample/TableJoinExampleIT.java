package ch.elca.flinksample;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class TableJoinExampleIT {
    private static final String randomSuffix = RandomStringUtils.randomAlphanumeric(12);
    private static final String INPUT_TOPIC1 = "table-join-input-1-" + randomSuffix;
    private static final String INPUT_TOPIC2 = "table-join-input-2-" + randomSuffix;
    private static final String OUTPUT_TOPIC = "table-join-output-" + randomSuffix;

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.0")
    );

    @BeforeAll
    public static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = List.of(
                    new NewTopic(INPUT_TOPIC1, 1, (short) 1),
                    new NewTopic(INPUT_TOPIC2, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 1, (short) 1)
            );
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @RegisterExtension
    public static MiniClusterExtension flinkCluster =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    static KafkaConsumer<String, String> kafkaConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    static KafkaProducer<String, String> kafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void logFlinkClusterStatus(ClusterClient<?> client) throws Exception {
        // List all jobs known to the cluster
        client.listJobs().thenAccept(jobIds -> {
            System.out.println("Jobs currently in the MiniCluster:");
            for (JobStatusMessage jobId : jobIds) {
                System.out.println("  • " + jobId);
            }
        });

        // For each job, fetch its status
        for (JobStatusMessage jobStatusMessage : client.listJobs().get()) {
            JobStatus status = client.getJobStatus(jobStatusMessage.getJobId()).get();
            System.out.println("Job " + jobStatusMessage + " is in state " + status);
            // Print stack trace for failed jobs
            if (status.equals(JobStatus.FAILED)) {
                JobResult result = client.requestJobResult(jobStatusMessage.getJobId()).get();
                result.getSerializedThrowable().ifPresent(t -> {
                    System.out.println("=== Job Failure Stacktrace ===");
                    System.out.println(t.getFullStringifiedStackTrace());
                });
            }
        }
    }

    @Test
    void test_join(@InjectClusterClient ClusterClient<?> client) throws Exception {
        TableJoinExample.tableJoin(KAFKA_CONTAINER.getBootstrapServers(), INPUT_TOPIC1, INPUT_TOPIC2, OUTPUT_TOPIC);
        try (KafkaProducer<String, String> producer = kafkaProducer()) {
            Stream.of(
                    new ProducerRecord<>(INPUT_TOPIC1, "key1", "{\"a\": 1}"),   // key1: {"a": 1, "b": null}
                    new ProducerRecord<>(INPUT_TOPIC2, "key2", "{\"b\": 2}"),    // key2: {"a": null, "b": 2}
                    new ProducerRecord<>(INPUT_TOPIC2, "key2", "{\"b\": -2}"),    // key2: {"a": null, "b": -2}
                    new ProducerRecord<>(INPUT_TOPIC2, "key1", "{\"b\": 1}"),    // key1: {"a": 1, "b": 1} => 1
                    new ProducerRecord<>(INPUT_TOPIC2, "key1", "{\"b\": -1}"),    // key1: {"a": 1, "b": -1} => -1
                    new ProducerRecord<>(INPUT_TOPIC1, "key2", "{\"a\": 2}")    // key2: {"a": 2, "b": -2} => -4
            ).forEach(record -> {
                try {
                    producer.send(record).get();    // block asynchronous operation
                    Thread.sleep(500);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        logFlinkClusterStatus(client);

        try (KafkaConsumer<String, String> consumer = kafkaConsumer()) {
            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            await()
                    .pollInterval(Duration.ofSeconds(1))
                    .timeout(Duration.ofSeconds(10))
                    .untilAsserted(
                            () -> {
                                consumer.poll(Duration.ofSeconds(1)).forEach(records::add);
                                assertThat(records).satisfiesExactly(
                                        record -> assertThat(record)
                                                .hasFieldOrPropertyWithValue("key", "key1")
                                                .hasFieldOrPropertyWithValue("value", "{\"product\":1}"),
                                        record -> assertThat(record)
                                                .hasFieldOrPropertyWithValue("key", "key1")
                                                .hasFieldOrPropertyWithValue("value", "{\"product\":-1}"),
                                        record -> assertThat(record)
                                                .hasFieldOrPropertyWithValue("key", "key2")
                                                .hasFieldOrPropertyWithValue("value", "{\"product\":-4}")
                                );
                            }
                    );
        }
    }
}
