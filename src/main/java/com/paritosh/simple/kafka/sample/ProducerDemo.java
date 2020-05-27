package com.paritosh.simple.kafka.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    static String bootStrapServer = "127.0.0.1:9092";
    public static void main(String[] args) {
        System.out.println("Hello World");

        //create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName()) ;
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        //Create producer Record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("first_topic" , "Hello World");
        //send data - ASYNCH
        producer.send(producerRecord);
        //flush
        producer.flush();
        //close
        producer.close();

    }
}
