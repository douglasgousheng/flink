package com.douglas.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author douglas
 * @create 2020-11-18 18:05
 */
public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        inputDS.addSink(new JdbcSink());

        env.execute();
    }
    public static class JdbcSink extends RichSinkFunction<String>{
        Connection connection =null;
        PreparedStatement preparedStatement =null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection=DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");
            preparedStatement=connection.prepareStatement("INSERT INTO sensor(id,temp)VALUES(?,?)ON DUPLICATE KEY UPDATE temp=?");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] fields = value.split(",");
            preparedStatement.setString(1,fields[0]);
            preparedStatement.setDouble(2,Double.parseDouble(fields[2]));
            preparedStatement.setDouble(3,Double.parseDouble(fields[2]));

            preparedStatement.execute();

        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
