package com.douglas.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author douglas
 * @create 2020-11-23 15:48
 */
public class FlinkSQL_Test4_TableTranToDStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1. 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);




    }
}
