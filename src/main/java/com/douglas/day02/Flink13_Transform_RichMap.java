package com.douglas.day02;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-17 18:49
 */
public class Flink13_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> fileDS = env.readTextFile("sensor");

        SingleOutputStreamOperator<SensorReading> map = fileDS.map(new MyRichMapFunc());

        map.print();

        env.execute();
    }

    public static class MyRichMapFunc extends RichMapFunction<String, SensorReading>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open方法被调用");
        }

        @Override
        public SensorReading map(String s) throws Exception {

            String[] fields = s.split(",");
            return new SensorReading(fields[0],
                    Long.parseLong(fields[1]),
                    Double.parseDouble(fields[2]));
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close方法被调用");
        }
    }

}
