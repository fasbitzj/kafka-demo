package com.ucl.kafka;

import com.ucl.kafka.common.KafkaUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by jiang.zheng on 2017/11/6.
 */
public class ProducerTest {

    public static void main(String[] s) {
        try {

            Producer<String, String> producer = KafkaUtil.getProducer();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    sendRecord(producer, "replication-threadA");
                }
            }).start();

            sendRecord(producer, "replication-threadB");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void sendRecord(Producer producer, String topic) {
        int i = 0;
        while(true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(i), "topic:" + topic + ",this is message:"+i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        e.printStackTrace();
                    System.out.println("topic:" + topic + ", message send to partition " + metadata.partition() + ", offset: " + metadata.offset());
                }
            });
            i++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
