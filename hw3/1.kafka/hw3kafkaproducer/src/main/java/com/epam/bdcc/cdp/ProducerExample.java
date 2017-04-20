package com.epam.bdcc.cdp;

/**
 * Created by Volodymyr_Novostavsk on 20-Apr-17.
 * used tutorials:
 *   https://insight.io/github.com/apache/kafka/blob/HEAD/examples/src/main/java/kafka/examples/Producer.java
 *   https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
 */

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerExample {
    private Properties props;
    private String topicName;

    public ProducerExample(String topicName){
        // create instance for properties to access producer configs
        this.props = new Properties();

        //ToDO need to read properties from file. use this article -
        // http://crunchify.com/java-properties-file-how-to-read-config-properties-values-in-java/

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //Topic to send to
        this.topicName = topicName;
    }

    //send n Fibonacci numbers
    public void sendExample(int n){
        //create Producer instance
        Producer<String, String> producer = new KafkaProducer<String, String>(this.props);

        //calc Fiboncacci number and send it to kafka in cycle
        for(int i = 0; i < n; i++)
            producer.send(new ProducerRecord<String, String>(this.topicName,
                    Integer.toString(i), Long.toString(Fibonacci.getFibonacci(i))));
        System.out.println("Message sent successfully");
        producer.close();
    }

    public static void main(String[] args) {
        // Check arguments length value
        if(args.length != 2){
            System.out.println("Enter topic name as the 1st param (String),\n" +
                    "and number of Fibonacci as 2nd (integer)");
            return;
        }
        //convert args into string (toppic) and integer (Fibonacci num)
        String topic = args[0].toString();
        int n = Integer.parseInt(args[1]);

        ProducerExample pe = new ProducerExample(topic);
        pe.sendExample(n);
    }
}