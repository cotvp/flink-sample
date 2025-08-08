package ch.elca.flinksample;

import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiExample {
    static final String BOOTSTRAP_SERVERS = "kafka:19092";

    public static void main(String[] args) {
        // Set up the Flink execution environment

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // define source
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .columnByMetadata("timestamp", DataTypes.TIMESTAMP_LTZ(3))
                        .columnByMetadata("partition", DataTypes.BIGINT())
                        .columnByMetadata("offset", DataTypes.BIGINT())
                        .column("key", DataTypes.STRING())
                        .column("a", DataTypes.BIGINT())
                        .column("b", DataTypes.BIGINT())
                        .build())
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS)
                .option(KafkaConnectorOptions.TOPIC.key(), "table-input")
                .option(KafkaConnectorOptions.PROPS_GROUP_ID, "table-consumer")
                .option(KafkaConnectorOptions.SCAN_STARTUP_MODE, KafkaConnectorOptions.ScanStartupMode.EARLIEST_OFFSET)
                .option(KafkaConnectorOptions.VALUE_FORMAT, "json")
                .option(KafkaConnectorOptions.KEY_FORMAT, "raw")
                .option(KafkaConnectorOptions.KEY_FIELDS.key(), "key")
                .option(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE, KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY)
                .build());

        // define sink
        tableEnv.executeSql("""
                CREATE TABLE SinkTable (
                  `key` STRING,
                  `a` BIGINT,
                  `b` BIGINT,
                  `c` BIGINT
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'table-output',
                  'properties.bootstrap.servers' = '%s',
                  'key.format' = 'raw',
                  'key.fields' = 'key',
                  'value.format' = 'json',
                  'value.fields-include' = 'EXCEPT_KEY'
                );
                """.formatted(BOOTSTRAP_SERVERS));

        Table input = tableEnv.from("SourceTable");

        input
                .select($("key"), $("a"), $("b"), $("a").plus($("b")).as("c"))
                .insertInto("SinkTable")
                .execute();
    }
}
