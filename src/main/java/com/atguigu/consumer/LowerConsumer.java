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
 * 根据指定的topic、Partition、offset来获取数据。
 * Created by 94478 on 2021/1/13.
 */
public class LowerConsumer {

    public static void main(String[] args){
        // 定义相关参数
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("192.168.40.101");
        brokers.add("192.168.40.102");
        brokers.add("192.168.40.103");
        // 端口号
        int port = 9092;
        // 主题
        String topic = "first";
        // 分区
        int partition = 1;
        //offset
        long offset = 2;

        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers,port,topic,partition,offset);
    }

    // 找分区leader
    private BrokerEndPoint findLeader(List<String> brokers,int port,String topic,int partition){
        for (String broker:brokers){
            SimpleConsumer getLeader = new SimpleConsumer(broker,port,1000,1024*4,"getLeader");
            // 创建一个主题元数据信息请求
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            // 获取主题元数据返回值
            TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);
            // 解析元数据返回值
            List<TopicMetadata> topicsMetadatas = metadataResponse.topicsMetadata();
            // 遍历主题元数据
            for (TopicMetadata topicMetadataum :topicsMetadatas){
                // 获取多个分区的元数据信息
                List<PartitionMetadata> partitionMetadatas = topicMetadataum.partitionsMetadata();
                // 遍历分区元数据
                for (PartitionMetadata partitionMetadata:partitionMetadatas){
                    if(partition == partitionMetadata.partitionId()){
                        return partitionMetadata.leader();
                    }
                }
            }
        }
        return null; // 返回null时，分区都挂了。
    }

    // 获取数据
    private void getData(List<String> brokers,int port,String topic,int partition,long offset){
        // 获取分区leader
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if (leader == null){
            return;
        }
        String leaderHost = leader.host();
        // 获取数据的消费者对象
        SimpleConsumer getData = new SimpleConsumer(leaderHost,port,1000,1024*4,"getData");
        // 创建获取数据的对象
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 100).build();
        // 获取数据返回值
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        // 解析返回值
        ByteBufferMessageSet messageAndOffSets = fetchResponse.messageSet(topic, partition);
        // 遍历并打印
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
