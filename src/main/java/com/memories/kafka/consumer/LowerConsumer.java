package com.memories.kafka.consumer;

import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LowerConsumer {

    public static void main(String[] args) {
        //kafka集群地址
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("192.168.2.135");
        brokers.add("192.168.2.136");
        brokers.add("192.168.2.137");

        //指定主题
        String topic = "second";

        //指定分区
        int partition = 0;

        //指定offset
        long offset = 2;

        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers,topic,partition,offset);


    }


    //获取leader
    public BrokerEndPoint getLeader(List<String> brokers, String topic, int partition){
        for (String broker : brokers) {
            SimpleConsumer simpleConsumer = new SimpleConsumer(broker,9092, 1000, 1024 * 4, "getLeader");
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            TopicMetadataResponse metadataResponse = simpleConsumer.send(topicMetadataRequest);
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
            for (TopicMetadata topicMetadata : topicsMetadata) {
                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
                for (PartitionMetadata partitionMetadata : partitionsMetadata) {
                    if(partitionMetadata.partitionId() == partition){
                        return partitionMetadata.leader();
                    }
                }
            }

        }
        return null;
    }

    //获取数据
    public void getData(List<String> brokers, String topic, int partition, long offset){

        BrokerEndPoint brokerEndPoint = getLeader(brokers, topic, partition);
        SimpleConsumer simpleConsumer = new SimpleConsumer(brokerEndPoint.host(),brokerEndPoint.port(),1000,1024 * 4,"getData");
        kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000).build();
        FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1 + "---" + new String(bytes));
        }

    }
}
