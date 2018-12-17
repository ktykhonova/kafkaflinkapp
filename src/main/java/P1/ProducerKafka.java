package P1;

import P2.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class ProducerKafka {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("Enter the interval of writing data into Kafka: ");
        Scanner scanner = new Scanner(System. in);
        int  requestPeriod = Integer.parseInt(scanner.nextLine());

        String response = new DataRequest().GetRequest().toString();
        response = response.substring(0, response.length() - 1).substring(1);
        String[] stringArray = response.split("},");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        for (int j = 0; j < 50; j++) {

            try {
                for (int i = 0; i < 3; i++) {
                    JSONObject myObject = new JSONObject(stringArray[i] + "}");

                    String toSend = "{'symbol':'" + myObject.getString("symbol") + "" + "','bid':'" + myObject.getDouble("bid") +
                            "','ask':'" + myObject.getDouble("ask") + "','price':'" + myObject.getDouble("price") +
                            "','timestamp':'" + myObject.getBigInteger("timestamp")+"'}";
                    System.out.println(toSend.replace("'","\""));
                    kafkaProducer.send(new ProducerRecord("currency-pair-" + (i + 1), Integer.toString(i),toSend.replace("'","\"")
                            ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {

            }
            TimeUnit.SECONDS.sleep(requestPeriod);

        }

    }
}

