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
         // ������Ϣ
         Properties props = new Properties();
         //kafka��Ⱥ
         props.put("bootstrap.servers", "192.168.40.102:9092");
         //��������id
         props.put("group.id", "test");
        // earliest ������0��ԭ������Ϊkafka��ɾ��֮ǰ��7�����1��G�����ݣ�   ��offset��Ϊ�����翪ʼ
         props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");// Ĭ�ϣ�latest
         //�����Զ��ύoffset
         props.put("enable.auto.commit", "true");
         //��topic�õ�����֮�󣬹�1����ύoffset.������õ����ݺ���1s֮�ڳ���ҵ��Ļ�����������ظ����ѣ�
         //�ڹ�˾�У�һ��Ҫ��ҵ��������ڻ��ύ��
         // �ύ��ʱ
         props.put("auto.commit.interval.ms", "100");
         // �����л�
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

         // ���������߶���
         KafkaConsumer consumer = new KafkaConsumer<>(props);
        //subscribe������
        consumer.subscribe(Collections.singletonList("second")); // ����
//        consumer.subscribe(Arrays.asList("second","first","third")); // ���

//        consumer.seek(new TopicPartition("first",0),2);  // ���Ҳ�����ظ�����

        // ��third������ʱ���ᱨһ�����󣬵��ǲ�Ӱ��ʣ��2�����ڵ�topic��ʹ�á�
        while (true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(100);// ������ʱ���ȡһ��
            for (ConsumerRecord<String,String> record:consumerRecords){
                System.out.println(record.topic()+" == "
                        + record.partition() +" == "
                        + record.value() +" == ");
            }
        }
    }
}
