package com.douglas.day05;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2020-11-21 11:27
 */
public class Flink01_State_Temp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        SingleOutputStreamOperator<SensorReading> sensorDataStream = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        KeyedStream<SensorReading, Tuple> keyedStream = sensorDataStream.keyBy("id");

        //4.判断温度是否跳变,如果跳变超过10度,则报警
        keyedStream.flatMap(new MyTempIncFunc());

    }

    public static class MyTempIncFunc extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{

        private ValueState<Double> lastTempState = null;

        @Override
        public void open(Configuration parameters) throws Exception {

            lastTempState=getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));

        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            Double lastTemp = lastTempState.value();
            Double curTemp = value.getTemp();

            if(lastTemp!=null && Math.abs(lastTemp-curTemp)>10.0){
                out.collect(new Tuple3<>(value.getId(),lastTemp,curTemp));
            }

            lastTempState.update(curTemp);

        }
    }

}
