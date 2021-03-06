package com.paritosh.simple.kafka.sample;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
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

        //send data with CallBack- ASYNCH
        producer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes everytime a record is successfully sent or exception is thrown
                if(e==null){
                    //record was sent successfully
                    logger.error("Received Metadata \n" + "Topic "+recordMetadata.topic()
                    +"\n "+"Partition: "+recordMetadata.partition()
                    +"\n"+"Offset: "+recordMetadata.offset()
                    +"\n"+"TimeStamp: "+recordMetadata.timestamp());
                }
                else{
                    logger.error("Error While Producing :", e);
                }
            }
        });
        //flush
        producer.flush();
        //close
        producer.close();

    }
}
