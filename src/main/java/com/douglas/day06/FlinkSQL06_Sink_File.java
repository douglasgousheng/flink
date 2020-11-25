package com.douglas.day06;

import com.douglas.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author douglas
 * @create 2020-11-23 18:14
 */
public class FlinkSQL06_Sink_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });
        Table table = tableEnv.fromDataStream(sensorDS);

        Table tableResult = table.groupBy("id").select("id,id.count as ct");

        tableEnv.createTemporaryView("socket", sensorDS);

        Table sqlResult = tableEnv.sqlQuery("select id,temp from socket where id='sensor_1'");

        tableEnv.connect(new FileSystem().path("sensorOut2"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("sensorOut2");

        tableEnv.insertInto("sensorOut2",sqlResult);

        env.execute();

    }
}
