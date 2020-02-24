package com.memories.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class CustomerStream {

    public static void main(String[] args) {
        //配置集群信息
        Properties properties = new Properties();
        properties.put("application.id","kafkaStream");
        properties.put("bootstrap.servers","192.168.2.135");
        properties.put("port",9092);

        //创建拓扑结构
        Topology topology = new Topology();
        topology.addSource("SOURCE","first");
        topology.addProcessor("PROCESSOR", LogProcessor::new, "SOURCE");
        topology.addSink("SINK","second","PROCESSOR");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.state();

    }

}
