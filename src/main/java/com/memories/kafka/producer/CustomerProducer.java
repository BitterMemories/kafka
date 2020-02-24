package com.memories.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

public class CustomerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.135:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("partitioner.class","com.memories.kafka.producer.CustomerPartition");
        ArrayList<String> list = new ArrayList<>();
        list.add("com.memories.kafka.intercetor.TimeIntercetor");
        list.add("com.memories.kafka.intercetor.CountIntercetor");
        props.put("interceptor.classes",list);

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<>("second", Integer.toString(i), Integer.toString(i)),
                    (metadata, exception) -> {
                        if(exception == null){
                            System.out.println(metadata.partition()+"---"+metadata.offset());
                        }else {
                            System.out.println("发送失败！");
                        }
                    });

        }

        producer.close();
    }
}
