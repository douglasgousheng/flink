package com.douglas.test;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.hash.Hash;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author douglas
 * @create 2020-11-18 8:59
 */
public class TextFile_WordDistinct {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> fileDS = env.readTextFile("input");
        SingleOutputStreamOperator<String> wordToOneDS = fileDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<String> filter = wordToOneDS.filter(new RichFilterFunction<String>() {

            Jedis jedis = null;
            String redisKey = "distinct";

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis("hadoop102", 6379);
            }

            @Override
            public boolean filter(String value) throws Exception {

                Boolean exsit = jedis.sismember(redisKey, value);

                if (!exsit) {
                    jedis.sadd(redisKey, value);
                }

                return !exsit;
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }
        });

        filter.print();
        env.execute();


    }

    public static class MyFlatMap implements FlatMapFunction<String, HashSet<String>> {

        HashSet<String> wordsCollect =new HashSet<String>();
        @Override
        public void flatMap(String s, Collector<HashSet<String>> collector) throws Exception {
            String[] words = s.split(" ");
            wordsCollect.addAll(Arrays.asList(words));

            collector.collect(wordsCollect);
        }
    }

}
