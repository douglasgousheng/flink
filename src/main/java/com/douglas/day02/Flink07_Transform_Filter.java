package com.douglas.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-17 16:52
 */
public class Flink07_Transform_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> fileDS = env.readTextFile("F:\\Users\\Administrator\\flink-200621\\sensor");

        SingleOutputStreamOperator<String> filter = fileDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                double temp = Double.parseDouble(s.split(",")[2]);
                return temp > 30.0D;
            }
        });

        filter.print();
        env.execute("Flink07_Transform_Filter");
    }
}
