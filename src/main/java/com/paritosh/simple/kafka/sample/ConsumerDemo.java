package com.paritosh.simple.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
   static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    public static void main(String[] args){

        String groupId = "my-fifth-application";
        Properties prop = new Properties();

        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , "127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        //subscribe cosumer to topic
        consumer.subscribe(Arrays.asList("first_topic"));

        //poll new Data
        while(true){

            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : consumerRecords){
                logger.info("Keys "+record.key()+" Val : "+record.value());
                logger.info("Partition: "+record.partition()+" Offset: "+record.offset());
            }


        }

    }

}
