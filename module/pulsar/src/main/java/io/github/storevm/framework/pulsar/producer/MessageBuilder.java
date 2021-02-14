package io.github.storevm.framework.pulsar.producer;

import io.github.storevm.framework.pulsar.model.Message;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface MessageBuilder<K, V> {
    /**
     * Used for set topic.
     * 
     * @param topic
     *            message topic
     * @return {@link MessageBuilder}
     */
    MessageBuilder<K, V> withTopic(String topic);

    /**
     * Used for message key.
     *
     * @param key
     *            message key
     * @return {@link MessageBuilder}
     */
    MessageBuilder<K, V> withKey(K key);

    /**
     * Used for set message body.
     *
     * @param t
     *            object need to be serialized.
     * @return {@link MessageBuilder}
     */
    MessageBuilder<K, V> withValue(V t);

    /**
     * Used for build message.
     *
     * @return message to be sent.
     */
    Message build();
}
