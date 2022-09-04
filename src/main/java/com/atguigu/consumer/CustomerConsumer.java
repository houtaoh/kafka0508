package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by 94478 on 2021/1/13.
 */
public class CustomerConsumer {

    public static void main(String[] args){
         // 配置信息
         Properties props = new Properties();
         //kafka集群
         props.put("bootstrap.servers", "192.168.40.102:9092");
         //消费者组id
         props.put("group.id", "test");
        // earliest 而不是0，原因是因为kafka会删除之前的7天或者1个G的数据，   把offset置为从最早开始
         props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");// 默认：latest
         //设置自动提交offset
         props.put("enable.auto.commit", "true");
         //从topic得到数据之后，过1秒才提交offset.（如果得到数据后在1s之内程序挂掉的化，就造成了重复消费）
         //在公司中，一般要等业务处理完成在会提交。
         // 提交延时
         props.put("auto.commit.interval.ms", "100");
         // 反序列化
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

         // 创建消费者对象
         KafkaConsumer consumer = new KafkaConsumer<>(props);
        //subscribe：订阅
        consumer.subscribe(Collections.singletonList("second")); // 单个
//        consumer.subscribe(Arrays.asList("second","first","third")); // 多个

//        consumer.seek(new TopicPartition("first",0),2);  // 这个也可以重复消费

        // 当third不存在时，会报一个错误，但是不影响剩下2个存在的topic的使用。
        while (true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(100);// 隔多少时间获取一次
            for (ConsumerRecord<String,String> record:consumerRecords){
                System.out.println(record.topic()+" == "
                        + record.partition() +" == "
                        + record.value() +" == ");
            }
        }
    }
}
