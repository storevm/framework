package io.github.storevm.framework.pulsar.model;

/**
 * @author Jack
 * @date 2020/07/06
 * @version 1.0.0
 */
public enum ServiceLifeState {
    /**
     * Service has been initialized.
     */
    INITIALIZED,

    /**
     * Service in starting.
     */
    STARTING,

    /**
     * Service in running.
     */
    STARTED,

    /**
     * Service is stopping.
     */
    STOPPING,

    /**
     * Service has been stopped.
     */
    STOPPED,
}
