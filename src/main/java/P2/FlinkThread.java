package P2;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.sql.*;
import java.util.Properties;

public class FlinkThread implements Runnable {
    private String topic_name;
    private int period;
    private Connection conn;
    private Statement stmt = null;


    public FlinkThread(String topic_name, int period) {
        this.topic_name = topic_name;
        this.period = period;
    }

    //mysql
    public String getCurrencyPairFromDatabase() {
        return "select id_pair\n" +
                "from currency_pairs\n" +
                "where name_pair = '" + this.topic_name + "';";
    }

    public String insertDataRowFromDatabase() {

        return "insert into currency_pairs (id_data_pair, minPrice, maxPrice, minBid, maxBid, minAsk, maxAsk, ref_id_pair)\n" +
                "values (default, ?, ?, ?, ?, ?, ?, "+this.topic_name+");";
    }

    public void run()  {
        System.out.println("run");
        try {

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "1");


            DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer011<>(this.topic_name,
                    new JSONKeyValueDeserializationSchema(false),
                    properties));
            stream.print();

            DataStream<Tuple8<String, Double, Double, Double, Double, Double, Double, Integer>> messagesFromKafka = stream
                    .flatMap(new FlatMapMinMax(this.period));
            stream.print();
            env.execute();

        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
