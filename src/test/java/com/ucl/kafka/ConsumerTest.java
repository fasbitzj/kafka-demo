package com.ucl.kafka;

import com.ucl.kafka.common.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by jiang.zheng on 2017/11/6.
 */
public class ConsumerTest {

    public static void main(String[] s){

        KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
        consumer.subscribe(Arrays.asList("replication-threadA", "replication-threadB"));
//        consumer.subscribe(Arrays.asList("replication-threadB"));
        consumer.seekToBeginning(new ArrayList<TopicPartition>());

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : records) {
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
            }
            //按分区读取数据
//              for (TopicPartition partition : records.partitions()) {
//                  List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//                  for (ConsumerRecord<String, String> record : partitionRecords) {
//                      System.out.println(record.offset() + ": " + record.value());
//                  }
//              }

        }

    }
}
