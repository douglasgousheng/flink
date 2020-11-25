package com.douglas.test;

import com.douglas.day01.Flink01_WordCount_Batch;
import com.douglas.day03.Flink04_Sink_MySQL;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author douglas
 * @create 2020-11-20 8:42
 */
public class KafkaToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id","consumer-group");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = kafkaDS.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        result.addSink(new MySQLSink());

        env.execute();


    }
    public static class MySQLSink extends RichSinkFunction<Tuple2<String, Integer>>{
        Connection connection =null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection= DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");
            insertStatement=connection.prepareStatement("INSERT INTO wordcount(word,`count`)VALUES(?,?)");
            updateStatement=connection.prepareStatement("UPDATE wordcount SET `count`=? WHERE word=?");
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            updateStatement.setInt(1,value.f1);
            updateStatement.setString(2,value.f0);
            updateStatement.execute();
            if(updateStatement.getUpdateCount()==0){
                insertStatement.setString(1,value.f0);
                insertStatement.setInt(2,value.f1);
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

