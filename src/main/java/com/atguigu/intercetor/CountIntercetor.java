package com.atguigu.intercetor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by 94478 on 2021/1/14.
 */
public class CountIntercetor implements ProducerInterceptor<String,String> {

    private int sucessCount = 0;

    private int errorCount = 0;


    /**
     * 这是一个核心方法：验证数据字段等。
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null){
            sucessCount++;
        }else{
            errorCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("发送成功："+sucessCount+"条");
        System.out.println("发送失败："+errorCount+"条");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
