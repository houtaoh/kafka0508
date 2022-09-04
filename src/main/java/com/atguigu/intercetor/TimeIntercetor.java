package com.atguigu.intercetor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by 94478 on 2021/1/14.
 */
public class TimeIntercetor implements ProducerInterceptor<String,String>{

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        System.out.println("houtaotao");
        return new ProducerRecord<String, String>(record.topic(),record.partition(), record.key(),System.currentTimeMillis()+","+record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
