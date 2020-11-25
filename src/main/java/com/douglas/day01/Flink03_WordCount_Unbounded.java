package com.douglas.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-16 14:55
 */
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


        DataStreamSource<String> lineDS = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = lineDS.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc()).setParallelism(3);

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDStream.keyBy(0);

        //5.计算聚合结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        //6.打印数据结果
        result.print();

        //7.启动任务
        env.execute("Flink03_WordCount_Unbounded");
    }
}
