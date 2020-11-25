package com.douglas.day07;

import com.douglas.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author douglas
 * @create 2020-11-24 20:08
 */
public class FlinkSQL10_Function_TableFunc {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据创建流,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        tableEnv.registerFunction("Split",new Split());

        Table tableResult = table
                .joinLateral("Split(id) as (word,length)")
                .select("id,word,length");

        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,word,length from sensor, " +
                "LATERAL TABLE(Split(id)) as T(word,length)");
        //7.转换为流打印数据
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        //8.执行
        env.execute();


    }
    public static class Split extends TableFunction<Tuple2<String,Integer>>{
        public void eval(String value){
            String[] split = value.split("_");
            for (String s : split) {
                collector.collect(new Tuple2<>(s,s.length()));
            }
        }
    }
}
