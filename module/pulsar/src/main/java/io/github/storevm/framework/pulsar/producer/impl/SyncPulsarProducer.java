package io.github.storevm.framework.pulsar.producer.impl;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import io.github.storevm.framework.pulsar.config.PulsarProducerConfig;
import io.github.storevm.framework.pulsar.model.Message;
import io.github.storevm.framework.pulsar.model.PulsarMessage;
import io.github.storevm.framework.pulsar.model.SendResult;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Slf4j
public class SyncPulsarProducer<K, V> extends PulsarProducer<K, V> {
    /**
     * constructor
     * 
     * @param client
     * @param config
     * @param key
     * @param value
     */
    public SyncPulsarProducer(PulsarClient client, PulsarProducerConfig config, K key, V value) {
        super(client, config, key, value);
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.Producer#send(io.github.storevm.framework.pulsar.model.Message)
     */
    @Override
    public SendResult send(Message message) {
        if (message != null && (message instanceof PulsarMessage)) {
            // 转换消息类型
            PulsarMessage<K, V> pm = (PulsarMessage<K, V>)message;
            if (producer != null) {
                try {
                    TypedMessageBuilder builder = toMessageBuilder(pm);
                    // 同步发送
                    MessageId id = builder.send();
                    return toSendResult(pm, id);
                } catch (PulsarClientException ex) {
                    log.error("同步发送Pulsar消息时发生异常", ex);
                    throw new RuntimeException("同步发送Pulsar消息时发生异常");
                }
            }
        }
        return null;
    }
}
