package com.memories.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {

    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //Kafka集群
        props.put("bootstrap.servers", "192.168.2.135:9092");
        //消费者组id
        props.put("group.id", "test");
        //是否从头消费
        props.put("auto.offset.reset","earliest");
        //自动提交offset
        props.put("enable.auto.commit", "true");
        //提交延时
        props.put("auto.commit.interval.ms", "1000");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        //指定topic
        //kafkaConsumer.subscribe(Collections.singletonList("second"));

        kafkaConsumer.assign(Collections.singletonList(new TopicPartition("second",1)));
        kafkaConsumer.seek(new TopicPartition("second",1),2);

        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.topic()+"---"+record.partition()+"---"+record.value());
            }
        }

    }
}
