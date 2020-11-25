package com.douglas.day06;

import com.douglas.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.table.descriptors.Json;

/**
 * @author douglas
 * @create 2020-11-23 18:41
 */
public class FlinkSQL08_Sink_ES_Append {
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

        Table tableResult = table.select("id,temp").where("id='sensor_1'");

        tableEnv.createTemporaryView("socket",sensorDS);

        Table sqlResult = tableEnv.sqlQuery("select id,temp from socket where id='sensor_1'");

        tableEnv.connect(new Elasticsearch()
        .version("6")
        .host("hadoop102",9200,"http")
                .index("flink_sql")
                .disableFlushOnCheckpoint()
                .bulkFlushMaxActions(1)
                .documentType("_doc"))
                .inAppendMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("EsPath");

        tableEnv.insertInto("EsPath",tableResult);

        tableEnv.toAppendStream(tableResult, Row.class).print();

        env.execute();



    }
}
