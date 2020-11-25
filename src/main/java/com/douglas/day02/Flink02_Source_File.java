package com.douglas.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-17 11:14
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sensorDS = env.readTextFile("F:\\Users\\Administrator\\flink-200621\\sensor");

        sensorDS.print();

        env.execute("Flink02_Source_File");
    }

}
