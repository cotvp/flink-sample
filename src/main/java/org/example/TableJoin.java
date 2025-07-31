package org.example;

import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

public class TableJoin {
    static final String BOOTSTRAP_SERVERS = "kafka:19092";

    public static void main(String[] args) {
        // Set up the Flink execution environment

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.createTemporaryTable("SourceTable1", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp")
                        .columnByMetadata("partition", DataTypes.BIGINT())
                        .columnByMetadata("offset", DataTypes.BIGINT())
                        .column("key", DataTypes.STRING())
                        .column("a", DataTypes.BIGINT())
                        .build())
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS)
                .option(KafkaConnectorOptions.TOPIC.key(), "table-join-input-1")
                .option(KafkaConnectorOptions.PROPS_GROUP_ID, "table-consumer")
                .option(KafkaConnectorOptions.SCAN_STARTUP_MODE, KafkaConnectorOptions.ScanStartupMode.EARLIEST_OFFSET)
                .option(KafkaConnectorOptions.VALUE_FORMAT, "json")
                .option(KafkaConnectorOptions.KEY_FORMAT, "raw")
                .option(KafkaConnectorOptions.KEY_FIELDS.key(), "key")
                .option(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE, KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY)
                .build());

        tableEnv.createTemporaryTable("SourceTable2", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp")
                        .columnByMetadata("partition", DataTypes.BIGINT())
                        .columnByMetadata("offset", DataTypes.BIGINT())
                        .column("key", DataTypes.STRING())
                        .column("b", DataTypes.BIGINT())
                        .build())
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS)
                .option(KafkaConnectorOptions.TOPIC.key(), "table-join-input-2")
                .option(KafkaConnectorOptions.PROPS_GROUP_ID, "table-consumer")
                .option(KafkaConnectorOptions.SCAN_STARTUP_MODE, KafkaConnectorOptions.ScanStartupMode.EARLIEST_OFFSET)
                .option(KafkaConnectorOptions.VALUE_FORMAT, "json")
                .option(KafkaConnectorOptions.KEY_FORMAT, "raw")
                .option(KafkaConnectorOptions.KEY_FIELDS.key(), "key")
                .option(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE, KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY)
                .build());

        tableEnv.createTemporaryTable("SinkTable", TableDescriptor.forConnector("upsert-kafka")
                .schema(Schema.newBuilder()
                        .column("key", DataTypes.STRING().notNull())
                        .column("product", DataTypes.BIGINT())
                        .primaryKeyNamed("pk_key", "key")
                        .build())
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS)
                .option(KafkaConnectorOptions.TOPIC.key(), "table-join-output")
                .option(KafkaConnectorOptions.VALUE_FORMAT, "json")
                .option(KafkaConnectorOptions.KEY_FORMAT, "raw")
                .option(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE, KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY)
                .build());

        Table input1 = tableEnv.sqlQuery("""
                SELECT key AS key1, a
                FROM (
                   SELECT key, a, event_time,
                     ROW_NUMBER() OVER (PARTITION BY key ORDER BY event_time DESC) AS row_num
                   FROM SourceTable1) AS latest_per_key
                 WHERE row_num = 1
                """);
        Table input2 = tableEnv.sqlQuery("""
                SELECT key AS key2, b
                FROM (
                   SELECT key, b, event_time,
                     ROW_NUMBER() OVER (PARTITION BY key ORDER BY event_time DESC) AS row_num
                   FROM SourceTable2) AS latest_per_key
                 WHERE row_num = 1
                """);

        input1
                .join(input2)
                .where($("key1").isEqual($("key2")))
                .select($("key1").as("key"), $("a").times($("b")).as("product"))
                .insertInto("SinkTable").execute();

    }
}
