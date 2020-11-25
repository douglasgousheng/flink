package com.douglas.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author douglas
 * @create 2020-11-23 15:40
 */
public class FlinkSQL_Test3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Kafka kafka = new Kafka()
                .version("0.11")
                .topic("test")
                .property("bootstrap.servers", "hadoop102:9092");

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE());

        tableEnv.connect(kafka).withFormat(new Csv()).withSchema(schema).createTemporaryTable("KafkaTable");

        Table table = tableEnv.sqlQuery("select id, temp from KafkaTable where id='sensor_1'");

        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
