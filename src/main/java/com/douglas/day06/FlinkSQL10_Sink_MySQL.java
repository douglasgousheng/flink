package com.douglas.day06;

import com.douglas.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author douglas
 * @create 2020-11-23 19:29
 */
public class FlinkSQL10_Sink_MySQL {
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
        Table table = tableEnv.fromDataStream(sensorDS);

        Table tableResult = table.groupBy("id").select("id,id.count as ct");

        String sinkDDL = "CREATE TABLE jdbcOutputTable (" +
                "id varchar(20) not null,"+
                "ct bigint not null"+
                ") WITH (" +
                "  'connector.type' = 'jdbc'," +
                "  'connector.url' = 'jdbc:mysql://hadoop102:3306/test', " +
                "  'connector.table' = 'sensor_count1'," +
                "  'connector.driver' = 'com.mysql.jdbc.Driver', " +
                "  'connector.username' = 'root', " +
                "  'connector.password' = '123456'," +
                "  'connector.write.flush.max-rows' = '1')";
        tableEnv.sqlUpdate(sinkDDL);
        tableEnv.insertInto("jdbcOutputTable",tableResult);

        env.execute();

    }
}
