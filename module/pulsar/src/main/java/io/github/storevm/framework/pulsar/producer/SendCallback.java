package io.github.storevm.framework.pulsar.producer;

import io.github.storevm.framework.pulsar.exception.OnExceptionContext;
import io.github.storevm.framework.pulsar.model.SendResult;

/**
 * @author Jack
 * @date 2021/02/14
 */
public interface SendCallback<T, V> {
    /**
     * When message send success, this method will be invoked.
     *
     * @param sendResult
     *            send message result.
     * @see SendResult
     */
    void onSuccess(final SendResult<T, V> result);

    /**
     * When message send failed, this method will be invoked.
     *
     * @param context
     *            send message exception context.
     * @see OnExceptionContext
     */
    void onException(final OnExceptionContext context);
}
