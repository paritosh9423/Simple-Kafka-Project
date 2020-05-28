package com.paritosh.simple.kafka.sample;

import com.sun.xml.internal.bind.v2.model.annotation.RuntimeAnnotationReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    public static void main(String[] args){
        new ConsumerDemoWithThread().run();
    }
    public ConsumerDemoWithThread(){

    }
    public void run()  {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(countDownLatch,"first_topic");
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
        }));
        try{
            countDownLatch.await();
        }catch (Exception e){
            logger.error("App got interuppted");
        }finally {
            logger.info("App is closing!!! ");
        }
    }


    public class ConsumerRunnable implements Runnable{
        private CountDownLatch countDownLatch = null;
        private KafkaConsumer<String,String> consumer ;
        public ConsumerRunnable(CountDownLatch countDownLatch , String topic){
            this.countDownLatch = countDownLatch;
            //consumer config
            String groupId = "my-fifth-application";
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , "127.0.0.1:9092");
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            this.consumer = new KafkaConsumer<String, String>(prop);
            this.consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            //poll for new Data
            try {
                while (true) {

                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Keys " + record.key() + " Val : " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            }
            catch (WakeupException ex){
                logger.info("ShutDown Called : ");
            }finally {
                consumer.close();
                countDownLatch.countDown();//tell main code
            }
        }

        public void shutdown(){
            consumer.wakeup();//interrupt consumer.poll() , throws a WakeUpException
        }
    }

}
