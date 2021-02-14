package io.github.storevm.framework.pulsar;

import io.github.storevm.framework.pulsar.model.ServiceLifeState;

/**
 * @author Jack
 * @date 2021/02/14
 */
public abstract class MessageManagement implements LifeCycle {
    private transient ServiceLifeState status = ServiceLifeState.INITIALIZED; // status

    /**
     * @see io.github.storevm.framework.pulsar.LifeCycle#isStarted()
     */
    @Override
    public boolean isStarted() {
        return status == ServiceLifeState.STARTED;
    }

    /**
     * @see io.github.storevm.framework.pulsar.LifeCycle#isClosed()
     */
    @Override
    public boolean isClosed() {
        return status == ServiceLifeState.STOPPED;
    }

    /**
     * @see io.github.storevm.framework.pulsar.LifeCycle#start()
     */
    @Override
    public void start() {
        if (!isStarted()) {
            doStart();
            status = ServiceLifeState.STARTED;
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.LifeCycle#shutdown()
     */
    @Override
    public void shutdown() {
        if (!isClosed()) {
            doClose();
            status = ServiceLifeState.STOPPED;
        }
    }

    /**
     * 执行启动
     */
    protected abstract void doStart();

    /**
     * 执行关闭
     */
    protected abstract void doClose();
}
