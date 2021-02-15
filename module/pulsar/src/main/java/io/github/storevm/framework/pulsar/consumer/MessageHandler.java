 package io.github.storevm.framework.pulsar.consumer;

import io.github.storevm.framework.pulsar.model.Action;
import io.github.storevm.framework.pulsar.model.Message;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface MessageHandler {
    Action consume(final Message message);
}
