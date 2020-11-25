package com.douglas.day02;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-17 16:42
 */
public class Flink05_Transform_Map {
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

        sensorDS.print();

        env.execute("Flink05_Transform_Map");

    }
}
