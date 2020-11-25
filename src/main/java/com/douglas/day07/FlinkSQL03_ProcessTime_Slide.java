package com.douglas.day07;

import com.douglas.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author douglas
 * @create 2020-11-24 18:39
 */
public class FlinkSQL03_ProcessTime_Slide {
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
//        Table result = table.window(Slide.over("5.seconds").every("2.seconds").on("pt").as("sw"))
//                .groupBy("id,sw")
//                .select("id,id.count");
//        Table result = table.window(Slide.over("5.rows").every("2.rows").on("pt").as("sw"))
//                .groupBy("id,sw")
//                .select("id,id.count");

        tableEnv.createTemporaryView("sensor", table);
        Table result = tableEnv.sqlQuery("select id,count(id) as ct from sensor " +
                "group by id,HOP(pt, INTERVAL '2' second, INTERVAL '6' second)");

        tableEnv.toAppendStream(result, Row.class).print();
        env.execute();

    }
}
