package io.github.storevm.framework.pulsar.producer;

import java.util.Properties;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface MessagingAccessPoint<K, V> {
    /**
     * Returns the target OMS specification version of the specified vendor implementation.
     *
     * @return the OMS version of implementation
     */
    String version();

    /**
     * Returns the attributes of this {@code MessagingAccessPoint} instance.
     * <p>
     * There are some standard attributes defined by OMS for {@code MessagingAccessPoint}:
     *
     * @return the attributes
     */
    Properties attributes();

    /**
     * Creates a new {@code Producer} for the specified {@code MessagingAccessPoint}.
     * 
     * @param topic
     * @param key
     * @param value
     * @param properties
     * @return the created {@code Producer}
     */
    Producer<K, V> createProducer(final String topic, final K key, final V value, final Properties properties)
        throws Exception;
}
