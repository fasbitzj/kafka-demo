package com.ucl.kafka.common;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Created by jiang.zheng on 2017/11/6.
 */
public class KafkaUtil {

    private static KafkaProducer<String, String> kp;
    private static KafkaConsumer<String, String> kc;

    public static KafkaProducer<String, String> getProducer() {
        if (kp == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "10.1.210.50:9092");
            props.put("acks", "0");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kp = new KafkaProducer<>(props);
        }
        return kp;
    }


    public static KafkaConsumer<String, String> getConsumer() {
        if(kc == null) {
            Properties props = new Properties();

            props.put("bootstrap.servers", "10.1.210.50:9092");
            props.put("group.id", "12");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kc = new KafkaConsumer<>(props);
        }

        return kc;
    }
}
