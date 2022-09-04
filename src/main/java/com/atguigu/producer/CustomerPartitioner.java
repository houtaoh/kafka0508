package com.atguigu.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * �Զ������
 * Created by 94478 on 2021/1/13.
 */
public class CustomerPartitioner implements Partitioner {

    private Map configMap = null;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Ĭ����0�ŷ�����
        return (int)key;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        configMap = configs;
    }
}
