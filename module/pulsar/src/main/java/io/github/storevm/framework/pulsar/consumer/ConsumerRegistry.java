package io.github.storevm.framework.pulsar.consumer;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface ConsumerRegistry {
    void addListener(String[] topics, MessageHandler listener);

    void shutdown();
}
