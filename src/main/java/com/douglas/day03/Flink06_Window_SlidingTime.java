package com.douglas.day03;

import com.douglas.day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author douglas
 * @create 2020-11-18 20:06
 */
public class Flink06_Window_SlidingTime {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        //DataStreamSource<String> input = env.readTextFile("input");
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        //5.简化版本滑动时间开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDStream = keyedStream.timeWindow(Time.seconds(6), Time.seconds(2));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowDStream.sum(1);

        sum.print();

        env.execute();
    }
}
