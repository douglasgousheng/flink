package com.douglas.day06;

import com.douglas.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author douglas
 * @create 2020-11-23 17:59
 */
public class FlinkSQL05_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<SensorReading> sensorDS = env.readTextFile("sensor/sensor.txt")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView("sensor",sensorDS);

        Table table = tableEnv.from("sensor");
        Table tableResult = table.groupBy("id").select("id,id.count,temp.avg");

        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sensor group by id");

        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult,Row.class).print("sqlResult");

        env.execute();


    }
}
