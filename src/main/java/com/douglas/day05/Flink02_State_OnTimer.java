package com.douglas.day05;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.swing.*;

/**
 * @author douglas
 * @create 2020-11-21 11:42
 */

/**
 * 连续十秒内，温度没有下降就报警。
 */
public class Flink02_State_OnTimer {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        SingleOutputStreamOperator<SensorReading> sensorDataStream = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        //3.分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDataStream.keyBy("id");

        keyedStream.process(new MyKeyedProcessFunc());
    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private ValueState<Double> lastTempState = null;
        private ValueState<Long> tsState = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
            tsState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long lastTs = tsState.value();
            long ts = ctx.timerService().currentProcessingTime();

            if(lastTs ==null){
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsState.update(ts);
            }else if(value.getTemp()<lastTemp){
                ctx.timerService().registerProcessingTimeTimer(ts);
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsState.update(ts);
            }

            lastTempState.update(value.getTemp());


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey()+"连续10秒温度没有下降");
            tsState.clear();

        }
    }
}
