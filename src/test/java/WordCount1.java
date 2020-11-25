import com.douglas.bean.SensorReading;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author douglas
 * @create 2020-11-24 8:35
 */
public class WordCount1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties));

//        tableEnv.connect(new Kafka()
//        .topic("test")
//        .version("0.11")
//        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
//        .property(ConsumerConfig.GROUP_ID_CONFIG,"testKafka"))
//                .withFormat(new Csv())
//                .withSchema(new Schema())
//                .createTemporaryTable("kafkaInput");
//
//        Table table = tableEnv.from("kafkaInput");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        //2.读取端口数据创建流,转换为JavaBean
        SingleOutputStreamOperator<Wordsum> wordsumDS = sum.map(tuple2 -> new Wordsum(tuple2.f0, tuple2.f1));

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(wordsumDS);

        tableEnv.createTemporaryView("socket", table);

        Table sqlResult = tableEnv.sqlQuery("select word,`count` from socket group by word,`count`");

        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("wordcount01")
                .disableFlushOnCheckpoint()
                .bulkFlushMaxActions(1)
                .documentType("_doc"))
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("count",DataTypes.INT()))
                .createTemporaryTable("EsPath");

        tableEnv.insertInto("EsPath",sqlResult);

        tableEnv.toRetractStream(sqlResult, Row.class).print();

        env.execute();

    }
}
