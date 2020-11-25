package com.douglas.day02;

import com.douglas.bean.SensorReading;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author douglas
 * @create 2020-11-17 14:25
 */
public class Flink04_Source_Customer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> mySourceDS = env.addSource(new CustomerSource());

        mySourceDS.print();
        env.execute("Flink04_Source_Customer");

    }

    public static class CustomerSource implements SourceFunction<SensorReading> {

        private boolean running = true;

        Random random=new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            HashMap<String,Double> tempMap = new HashMap<>();

            for (int i = 0; i < 10; i++) {
                tempMap.put("Sensor_"+i,50+random.nextGaussian()*20);
            }

            while (running){
                for (String id : tempMap.keySet()) {
                    Double temp = tempMap.get(id);

                    double newTemp = temp + random.nextGaussian();

                    ctx.collect(new SensorReading(id,System.currentTimeMillis(),newTemp));

                    tempMap.put(id,newTemp);

                }
                Thread.sleep(2000);

            }

        }

        @Override
        public void cancel() {
            running = false;

        }
    }

}
