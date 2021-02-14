package io.github.storevm.framework.pulsar.producer.impl;

import java.util.Properties;

import io.github.storevm.framework.pulsar.model.Message;
import io.github.storevm.framework.pulsar.model.Message.SystemPropKey;
import io.github.storevm.framework.pulsar.model.PulsarMessage;
import io.github.storevm.framework.pulsar.producer.MessageBuilder;

/**
 * @author Jack
 * @date 2021/02/14
 */
public class PulsarMessageBuilder<K, V> implements MessageBuilder<K, V> {
    private V value; // 消息体
    private K key;
    private String topic;

    /**
     * @see io.github.storevm.framework.pulsar.producer.MessageBuilder#withTopic(java.lang.String)
     */
    @Override
    public MessageBuilder<K, V> withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.MessageBuilder#withKey(java.lang.Object)
     */
    @Override
    public MessageBuilder<K, V> withKey(K key) {
        this.key = key;
        return this;
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.MessageBuilder#withValue(java.lang.Object)
     */
    @Override
    public MessageBuilder<K, V> withValue(V t) {
        this.value = t;
        return this;
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.MessageBuilder#build()
     */
    @Override
    public Message build() {
        PulsarMessage message = new PulsarMessage(topic, key, value);
        Properties properties = new Properties();
        properties.put(SystemPropKey.KEYCLASS, key.getClass().getName());
        properties.put(SystemPropKey.VALUECLASS, value.getClass().getName());
        message.setUserProperties(properties);
        return message;
    }

}
