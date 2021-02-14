package io.github.storevm.framework.pulsar;

import io.github.storevm.framework.pulsar.consumer.Consumer;
import io.github.storevm.framework.pulsar.producer.Producer;

/**
 * The {@code LifeCycle} defines a lifecycle interface for a OMS related service endpoint, like {@link Producer},
 * {@link Consumer}, and so on.
 * <p>
 * If the service endpoint class implements the {@code ServiceLifecycle} interface, most of the containers can manage
 * the lifecycle of the corresponding service endpoint objects easily.
 * <p>
 * Any service endpoint should support repeated restart if it implements the {@code ServiceLifecycle} interface.
 * 
 * @author Jack
 * @date 2021/02/14
 */
public interface LifeCycle {
    /**
     * Used to determine whether the current instance is started.
     *
     * @return if this instance has been started success, this method will return true, otherwise false.
     */
    boolean isStarted();

    /**
     * Used to determine whether the current instance is closed.
     *
     * @return if this instance has been stopped, this method will return true, otherwise false.
     */
    boolean isClosed();

    /**
     * Used for startup or initialization of a service endpoint. A service endpoint instance will be in a ready state
     * after this method has been completed.
     */
    void start();

    /**
     * Notify a service instance of the end of its life cycle. Once this method completes, the service endpoint could be
     * destroyed and eligible for garbage collection.
     */
    void shutdown();
}
