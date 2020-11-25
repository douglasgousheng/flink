package com.douglas.test;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Collections;
import java.util.Properties;

/**
 * @author douglas
 * @create 2020-11-18 8:43
 */
public class Source_kafka_Split {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<SensorReading> sensorDS = kafkaDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]));
            }
        });

        SplitStream<SensorReading> split = sensorDS.split(new OutputSelector<SensorReading>() {

            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30 ?
                        Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> low = split.select("low");
        DataStream<SensorReading> high = split.select("high");

        low.print("low");
        high.print("high");

        env.execute("Source_kafka_Split");


    }
}
