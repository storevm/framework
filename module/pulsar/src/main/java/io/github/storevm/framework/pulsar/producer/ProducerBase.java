package io.github.storevm.framework.pulsar.producer;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface ProducerBase<K, V> {
    /**
     * Create message builder, used for build message in a fluent way.
     *
     * @param <T>
     *            the class of message's payload
     * @return MessageBuilder
     */
    MessageBuilder<K, V> messageBuilder();
}
