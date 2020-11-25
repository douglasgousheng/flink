package com.douglas.day03;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author douglas
 * @create 2020-11-18 14:32
 */
public class Flink04_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileDS = env.readTextFile("F:\\Users\\Administrator\\flink-200621\\sensor");

        SingleOutputStreamOperator<SensorReading> sensorDS = fileDS.map(new MapFunction<String, SensorReading>() {

            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]));
            }
        });
        sensorDS.addSink(new MyJDBCSink());
        env.execute();
    }

    public static class MyJDBCSink extends RichSinkFunction<SensorReading> {
        Connection connection = null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            insertStatement = connection.prepareStatement("INSERT INTO sensor(id,temp) VALUES(?,?)");
            updateStatement = connection.prepareStatement("UPDATE sensor SET temp=? where id =?");

        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStatement.setDouble(1,value.getTemp());
            updateStatement.setString(2,value.getId());
            updateStatement.execute();
            if(updateStatement.getUpdateCount()==0){
                insertStatement.setString(1,value.getId());
                insertStatement.setDouble(2,value.getTemp());
                insertStatement.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }
    }
}
