package com.douglas.day02;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-17 16:58
 */
public class Flink08_Transform_Max {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> fileDS = env.readTextFile("F:\\Users\\Administrator\\flink-200621\\sensor");

        SingleOutputStreamOperator<SensorReading> sensorDS = fileDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        SingleOutputStreamOperator<SensorReading> maxResult = keyedStream.max("temp");
        SingleOutputStreamOperator<SensorReading> maxByResult = keyedStream.maxBy("temp");

        maxResult.print("max");
        maxByResult.print("maxBy");

        env.execute();


    }
}
