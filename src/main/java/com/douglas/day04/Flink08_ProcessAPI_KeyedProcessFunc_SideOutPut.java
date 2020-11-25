package com.douglas.day04;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author douglas
 * @create 2020-11-20 20:38
 */
public class Flink08_ProcessAPI_KeyedProcessFunc_SideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        SingleOutputStreamOperator<SensorReading> highDS = keyedStream.process(new MyKeyedProcessFunc());

        DataStream<String> mediumDS = highDS.getSideOutput(new OutputTag<String>("medium") {});
        DataStream<String> lowDS = highDS.getSideOutput(new OutputTag<String>("low") {});

        highDS.print("high");
        mediumDS.print("medium");
        lowDS.print("low");

        env.execute();

    }
    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple,SensorReading,SensorReading>{

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            Double temp = value.getTemp();
            if(temp>30.0D){
                out.collect(value);
            }else if(temp<=30.0D && temp>20){
                ctx.output(new OutputTag<String>("medium"){},value.getId());
            }else{
                ctx.output(new OutputTag<String>("low"){},value.getId());
            }
        }
    }
}
