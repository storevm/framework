package io.github.storevm.framework.pulsar.consumer.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.annotation.Value;

import io.github.storevm.framework.pulsar.config.PulsarClientConfig;
import io.github.storevm.framework.pulsar.config.PulsarConsumerConfig;
import io.github.storevm.framework.pulsar.consumer.ConsumerRegistry;
import io.github.storevm.framework.pulsar.consumer.MessageHandler;

/**
 * @author Jack
 * @date 2021/02/14
 */
public class PulsarConsumerRegistry implements ConsumerRegistry {
    private Map<String, PulsarConsumer> consumers = new HashMap();
    @Value("${spring.pulsar.consumer.concurrency:1}")
    private int concurrency = 1; // 消费者工作线程

    /**
     * 注册消费者
     * 
     * @param client
     * @param config
     */
    public void register(PulsarClient client, PulsarClientConfig config) throws Exception {
        Set<Entry<String, PulsarConsumerConfig>> set = config.getConsumers().entrySet();
        Iterator<Entry<String, PulsarConsumerConfig>> it = set.iterator();
        while (it.hasNext()) {
            Entry<String, PulsarConsumerConfig> entry = it.next();
            PulsarConsumer consumer = new PulsarConsumer(client, entry.getValue());
            consumer.afterPropertiesSet();
            consumer.setConcurrency(concurrency); // 设置线程数
            consumers.put(entry.getValue().getTopic(), consumer);
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.consumer.ConsumerRegistry#addListener(java.lang.String[],
     *      io.github.storevm.framework.pulsar.consumer.MessageHandler)
     */
    @Override
    public void addListener(String[] topics, MessageHandler listener) {
        Set<Entry<String, PulsarConsumer>> set = consumers.entrySet();
        Iterator<Entry<String, PulsarConsumer>> it = set.iterator();
        while (it.hasNext()) {
            it.next().getValue().addListener(listener, topics);
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.consumer.ConsumerRegistry#shutdown()
     */
    @Override
    public void shutdown() {
        Set<Entry<String, PulsarConsumer>> set = consumers.entrySet();
        Iterator<Entry<String, PulsarConsumer>> it = set.iterator();
        while (it.hasNext()) {
            it.next().getValue().shutdown();
        }
    }

}
