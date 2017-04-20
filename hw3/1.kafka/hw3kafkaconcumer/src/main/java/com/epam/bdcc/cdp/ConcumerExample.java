package com.epam.bdcc.cdp;

/**
 * Created by Volodymyr_Novostavsk on 20-Apr-17.
 * used tutorials:
 *   https://insight.io/github.com/apache/kafka/blob/HEAD/examples/src/main/java/kafka/examples/Producer.java
 *   https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
 */

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConcumerExample {
    private Properties props;
    private String topicName;

    public ConcumerExample(String topicName){
        //Kafka consumer configuration settings
        this.props = new Properties();
        //ToDO need to read properties from file. use this article -
        // http://crunchify.com/java-properties-file-how-to-read-config-properties-values-in-java/
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //Topic to send to
        this.topicName = topicName;
    }


    public ConcumerExample listenExample(int m){
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(this.props);

        //Kafka Consumer subscribes to topic
        consumer.subscribe(Arrays.asList(this.topicName));
//        System.out.println("Subscribed to topic - " + topicName);

        //sum value of Fibonnaci numbers
        long result = 0;

        //listen in cycle, calc sum of numbers and output for every M-th value
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records){
                result += Long.parseLong(record.value());
                if(Integer.parseInt(record.key()) % m == 0){
                    System.out.println("num - " + record.key() + "; sum - " + result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.out.println("Enter topic name as the 1st param (String),\n" +
                    "and m - number to provide output as 2nd (integer)");
            return;
        }
        //convert args into string (toppic) and integer (Fibonacci num)
        String topic = args[0].toString();
        int m = Integer.parseInt(args[1]);

        //create Consumer and listed to topic
        ConcumerExample ce = new ConcumerExample(topic);
        ce.listenExample(m);
    }
}
