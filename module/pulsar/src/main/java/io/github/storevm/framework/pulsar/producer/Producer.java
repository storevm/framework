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
     * 
     * @param sendCallback
     */
    void setCallback(final SendCallback<K, V> callback);

    /**
     * 
     * @param delay
     */
    void setDelay(long delay);
}
