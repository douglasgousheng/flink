package com.douglas.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-16 11:54
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDataStream = env.readTextFile("D:\\gitproject\\flink-200621\\input");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDataStream = lineDataStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDataStream = wordToOneDataStream.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDataStream.sum(1);

        result.print();

        env.execute("Flink02_WordCount_Bounded");
    }
}
