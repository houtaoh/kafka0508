package com.atguigu.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Created by 94478 on 2021/1/14.
 */
public class KafkaStream {
    public static void main(String[] args) {
        // 创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();
        // 创建配置文件
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.254.102:9092");
        properties.put("application.id","kafkaId");
        // 构建拓扑结构
        builder.addSource("SOURCE","first")
               .addProcessor("PROCESSOR", new ProcessorSupplier() {
                   @Override
                   public Processor get() {
                       return new LogProcessor();
                   }
               }, "SOURCE")
               .addSink("SINK","second","PROCESSOR");
        KafkaStreams kafkaStreams = new KafkaStreams(builder,properties);
        kafkaStreams.start();




    }
}
