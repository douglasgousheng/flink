package com.douglas.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author douglas
 * @create 2020-11-23 20:06
 */
public class FlinkSQL13_PorcessTime_DDL {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        String sinkDDL = "CREATE TABLE dataTable (\n" +
                "  id varchar(20) not null," +
                "  ts bigint," +
                "  temp double,"+
                "  ps AS PROCTIME()" +
                ") WITH (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'sensor', " +
                " 'format.type' = 'csv')";
        bsTableEnv.sqlUpdate(sinkDDL);

        Table table = bsTableEnv.from("dataTable");
        table.printSchema();
    }
}
