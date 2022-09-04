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
        // Kafka����˵��������Ͷ˿ں�
        props.put("bootstrap.servers", "192.168.40.102:9092");
        // �ȴ����и����ڵ��Ӧ��
        //props.put("acks", "all");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        // ��Ϣ��������Դ���
        props.put("retries", 0);
        // һ����Ϣ�����С
        props.put("batch.size", 16384);
        // ���������
        ArrayList list = new ArrayList<String>();
        list.add("com.atguigu.intercetor.TimeIntercetor");
        list.add("com.atguigu.intercetor.CountIntercetor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);
        // ������ʱ
        props.put("linger.ms", 1);
        // ���ͻ������ڴ��С
        props.put("buffer.memory", 33554432);
        // key���л�
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value���л�
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // �Զ������
        //props.put("partitioner.class", "com.atguigu.producer.CustomerPartitioner");
//        ProducerConfig
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // ѭ����������
//        for (int i = 0; i < 10 ; i++) {
//            // key:topic      value:ֵ
//            producer.send(new ProducerRecord<String, String>("first",String.valueOf(i)));
//        }

        // ѭ����������
        // �������ڶ�ȡ���ݵ�ʱ����������һ���������ڻ�������һ��������
       for (int i = 0; i < 10 ; i++) {
           producer.send(new ProducerRecord<String, String>("second", String.valueOf(i)), (metadata, exception) -> {
               if (exception == null){
                   System.out.println(metadata.partition() + "--" + metadata.offset());
               }else{
                   System.out.println("����ʧ��");
               }
           });
       }
        // ����̨����
//        while (true){
//            Scanner input=new Scanner(System.in);
//            String str=input.next();
//            producer.send(new ProducerRecord<String, String>("second", str), (metadata, exception) -> {
//                   if (exception == null){
//                       System.out.println(metadata.partition() + "--" + metadata.offset());
//                   }else{
//                       System.out.println("����ʧ��");
//                   }
//               });
//            }
        // �ر���Դ
        producer.close();
    }
}




















