package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set up the Flink table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create source table to read from Kafka
        String create_source_table = "CREATE TABLE source_table (\n" +
                "    field1 STRING,\n" +
                "    field2 STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'input-topic',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9093',\n" +
                "    'properties.group.id' = 'my-group',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(create_source_table);

        // Create sink table to write to Kafka
        String create_sink_table = "CREATE TABLE sink_table (\n" +
                "    field1 STRING,\n" +
                "    field2 STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'output-topic',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9093',\n" +
                "    'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(create_sink_table);

        // Define the SQL query to process the data
        String insertQuery = "INSERT INTO sink_table " +
                "SELECT field1, field2 " +
                "FROM source_table " +
                "WHERE field2 = 'correct'";

        tableEnv.executeSql(insertQuery);
    }
}