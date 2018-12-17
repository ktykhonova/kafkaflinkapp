package P1;

//output

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class ConsumerKafka {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer consumerKafka = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("currency-pair-1");
        topics.add("currency-pair-2");
        topics.add("currency-pair-3");
        consumerKafka.subscribe(topics);
        try{
            while (true){
                ConsumerRecords<String, String> records = consumerKafka.poll(10);
                for (ConsumerRecord record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            consumerKafka.close();
        }
    }
}
