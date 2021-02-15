package io.github.storevm.framework.pulsar.consumer;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface Consumer {
    /**
     * 
     * @param listener
     * @param topics
     */
    void addListener(MessageHandler listener, String... topics);

    /**
     * Subscribe to messages, which can be filtered using SQL expressions.
     */
    void subscribe();

    void setConcurrency(int concurrency);

    boolean isStarted();

    boolean isClosed();

    void start();

    void shutdown();
}
