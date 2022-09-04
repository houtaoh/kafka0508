package com.atguigu.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Created by 94478 on 2021/1/14.
 */
public class LogProcessor implements Processor<byte[],byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;
    }

    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        // 获取一行数据
        String line = new String(bytes2);
        // 业务处理
        line = line.replace(">>>","");
        bytes2 = line.getBytes();
        context.forward(bytes,bytes2);
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
