package com.douglas.day07;

import com.douglas.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author douglas
 * @create 2020-11-24 19:29
 */
public class FlinkSQL07_ProcessTime_Over {
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

        Table result1 = table.window(Over.partitionBy("id").orderBy("pt").as("ow"))
                .select("id,id.count over ow");

        Table result2 = table.window(Over.partitionBy("id").orderBy("pt").preceding("3.rows").as("ow"))
                .select("id,id.count over ow");

        tableEnv.createTemporaryView("sensor",table);
        Table result = tableEnv.sqlQuery("select id,count(id) " +
                "over(partition by id order by pt) as ct " +
                "from sensor");

//        tableEnv.toRetractStream(result1, Row.class).print();
//        tableEnv.toRetractStream(result2, Row.class).print();
        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();


    }
}
