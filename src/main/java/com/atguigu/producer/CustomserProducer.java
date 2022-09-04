package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by 94478 on 2021/1/9.
 */
public class CustomserProducer {
    public static void main(String[] args){
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "192.168.40.102:9092");
        // 等待所有副本节点的应答
        //props.put("acks", "all");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 添加拦截器
        ArrayList list = new ArrayList<String>();
        list.add("com.atguigu.intercetor.TimeIntercetor");
        list.add("com.atguigu.intercetor.CountIntercetor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义分区
        //props.put("partitioner.class", "com.atguigu.producer.CustomerPartitioner");
//        ProducerConfig
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // 循环发送数据
//        for (int i = 0; i < 10 ; i++) {
//            // key:topic      value:值
//            producer.send(new ProducerRecord<String, String>("first",String.valueOf(i)));
//        }

        // 循环发送数据
        // 消费者在读取数据的时候，先消费完一个分区，在会消费另一个分区。
       for (int i = 0; i < 10 ; i++) {
           producer.send(new ProducerRecord<String, String>("second", String.valueOf(i)), (metadata, exception) -> {
               if (exception == null){
                   System.out.println(metadata.partition() + "--" + metadata.offset());
               }else{
                   System.out.println("发送失败");
               }
           });
       }
        // 控制台输入
//        while (true){
//            Scanner input=new Scanner(System.in);
//            String str=input.next();
//            producer.send(new ProducerRecord<String, String>("second", str), (metadata, exception) -> {
//                   if (exception == null){
//                       System.out.println(metadata.partition() + "--" + metadata.offset());
//                   }else{
//                       System.out.println("发送失败");
//                   }
//               });
//            }
        // 关闭资源
        producer.close();
    }
}




















