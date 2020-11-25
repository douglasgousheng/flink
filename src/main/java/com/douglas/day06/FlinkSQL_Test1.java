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
 * @create 2020-11-23 15:13
 */
public class FlinkSQL_Test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> readTextFile = env.readTextFile("sensor");

        SingleOutputStreamOperator<SensorReading> sensorDataStream = readTextFile.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(sensorDataStream);

        //6.转换数据
        //6.1 使用TableAPI转换数据
        Table result = table.select("id,temp").filter("id='sensor_1'");

        tableEnv.createTemporaryView("sensor",sensorDataStream);

        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor where id='sensor_1'");

        tableEnv.toAppendStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult,Row.class).print("sql");

        env.execute();


    }
}
