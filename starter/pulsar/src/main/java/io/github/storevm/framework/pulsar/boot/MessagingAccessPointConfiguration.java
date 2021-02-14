package io.github.storevm.framework.pulsar.boot;

/**
 * @author Jack
 * @date 2020/07/02
 * @version 1.0.0
 */
public abstract class MessagingAccessPointConfiguration {
    /**
     * 加载的配置参数
     */
    private final PulsarConfigProperties properties;

    /**
     * constructor
     * 
     * @param properties
     */
    protected MessagingAccessPointConfiguration(PulsarConfigProperties properties) {
        this.properties = properties;
    }

    /**
     * 返回配置参数
     * 
     * @return
     */
    protected final PulsarConfigProperties getProperties() {
        return this.properties;
    }
}
