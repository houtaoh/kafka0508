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
     * ����һ�����ķ�������֤�����ֶεȡ�
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
        System.out.println("���ͳɹ���"+sucessCount+"��");
        System.out.println("����ʧ�ܣ�"+errorCount+"��");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
