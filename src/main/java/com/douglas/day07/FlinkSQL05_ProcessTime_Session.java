package com.douglas.day07;

import com.douglas.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author douglas
 * @create 2020-11-24 19:03
 */
public class FlinkSQL05_ProcessTime_Session {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据创建流,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,pt.proctime");

        Table result = table.window(Session.withGap("5.seconds").on("pt").as("sw"))
                .groupBy("id,sw")
                .select("id,id.count");



        tableEnv.createTemporaryView("sensor",table);
        Table result4 = tableEnv.sqlQuery(
                "SELECT id,count(id) as ct from sensor " +
                        " GROUP BY id, SESSION(pt, INTERVAL '5' second)");

        tableEnv.toAppendStream(result, Row.class).print();
        tableEnv.toAppendStream(result4, Row.class).print();

        env.execute();
    }
}
