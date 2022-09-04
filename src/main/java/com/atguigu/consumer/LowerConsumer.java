package com.atguigu.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ����ָ����topic��Partition��offset����ȡ���ݡ�
 * Created by 94478 on 2021/1/13.
 */
public class LowerConsumer {

    public static void main(String[] args){
        // ������ز���
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("192.168.40.101");
        brokers.add("192.168.40.102");
        brokers.add("192.168.40.103");
        // �˿ں�
        int port = 9092;
        // ����
        String topic = "first";
        // ����
        int partition = 1;
        //offset
        long offset = 2;

        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers,port,topic,partition,offset);
    }

    // �ҷ���leader
    private BrokerEndPoint findLeader(List<String> brokers,int port,String topic,int partition){
        for (String broker:brokers){
            SimpleConsumer getLeader = new SimpleConsumer(broker,port,1000,1024*4,"getLeader");
            // ����һ������Ԫ������Ϣ����
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            // ��ȡ����Ԫ���ݷ���ֵ
            TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);
            // ����Ԫ���ݷ���ֵ
            List<TopicMetadata> topicsMetadatas = metadataResponse.topicsMetadata();
            // ��������Ԫ����
            for (TopicMetadata topicMetadataum :topicsMetadatas){
                // ��ȡ���������Ԫ������Ϣ
                List<PartitionMetadata> partitionMetadatas = topicMetadataum.partitionsMetadata();
                // ��������Ԫ����
                for (PartitionMetadata partitionMetadata:partitionMetadatas){
                    if(partition == partitionMetadata.partitionId()){
                        return partitionMetadata.leader();
                    }
                }
            }
        }
        return null; // ����nullʱ�����������ˡ�
    }

    // ��ȡ����
    private void getData(List<String> brokers,int port,String topic,int partition,long offset){
        // ��ȡ����leader
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if (leader == null){
            return;
        }
        String leaderHost = leader.host();
        // ��ȡ���ݵ������߶���
        SimpleConsumer getData = new SimpleConsumer(leaderHost,port,1000,1024*4,"getData");
        // ������ȡ���ݵĶ���
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 100).build();
        // ��ȡ���ݷ���ֵ
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        // ��������ֵ
        ByteBufferMessageSet messageAndOffSets = fetchResponse.messageSet(topic, partition);
        // ��������ӡ
        for (MessageAndOffset messageAndOffset :messageAndOffSets){
            long offset1 = messageAndOffset.offset();
            Message message = messageAndOffset.message();
            ByteBuffer payload  = message.payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            try {
                System.out.println(offset1 + "--" + new String(bytes,"utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }
}
