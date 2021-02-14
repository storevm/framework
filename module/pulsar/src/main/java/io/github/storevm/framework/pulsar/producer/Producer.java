package io.github.storevm.framework.pulsar.producer;

import io.github.storevm.framework.pulsar.model.Message;
import io.github.storevm.framework.pulsar.model.SendResult;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface Producer<K, V> extends ProducerBase<K, V> {
    /**
     * Sends a message to the specified destination synchronously, the destination should be preset to
     * {@link Message#setTopic(String)}, other header fields as well.
     *
     * @param message
     *            a message will be sent.
     * @return the successful {@code SendResult}.
     */
    SendResult send(final Message message);

    /**
     * Sends a message to the specified destination asynchronously, the destination should be preset to
     * {@link Message#setTopic(String)}, other header fields as well.
     * <p>
     * The returned {@code Promise} will have the result once the operation completes, and the registered
     * {@link SendCallback} will be invoked, either because the operation was successful or because of an error.
     *
     * @param message
     *            a message will be sent.
     * @param sendCallback
     *            {@link SendCallback}
     */
    void sendAsync(final Message message, final SendCallback sendCallback);

    /**
     * 发送延迟消息
     * 
     * @param message
     * @param delay
     * @return
     */
    SendResult delayedDelivery(final Message message, long delay);

    void shutdown();
}
