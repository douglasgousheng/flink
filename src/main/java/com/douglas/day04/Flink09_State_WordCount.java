package com.douglas.day04;

import com.douglas.day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2020-11-20 20:51
 */
public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMap.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private ValueState<Integer> countState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class,0));
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                Integer count = countState.value();

                count++;

                countState.update(count);

                return new Tuple2<>(value.f0, count);
            }
        });

        result.print();

        env.execute();
    }
}
