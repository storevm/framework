package io.github.storevm.framework.pulsar.producer.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import io.github.storevm.framework.pulsar.config.PulsarClientConfig;
import io.github.storevm.framework.pulsar.consumer.impl.PulsarConsumerRegistry;
import io.github.storevm.framework.pulsar.model.Message.SystemPropKey;
import io.github.storevm.framework.pulsar.model.ProducerTypeEnum;
import io.github.storevm.framework.pulsar.producer.MessagingAccessPoint;
import io.github.storevm.framework.pulsar.producer.Producer;

/**
 * @author Jack
 * @date 2021/02/14
 */
public class PulsarMessagingAccessPoint<K, V> implements MessagingAccessPoint<K, V>, InitializingBean, DisposableBean {
    private PulsarClient client; // Pulsar client instance
    private PulsarClientConfig config; // configuration
    private Map<String, PulsarProducer<K, V>> cache = new HashMap();
    private PulsarConsumerRegistry registry;
    private Properties properties;

    /**
     * constructor
     * 
     * @param config
     * @param registry
     */
    public PulsarMessagingAccessPoint(PulsarClientConfig config, PulsarConsumerRegistry registry) {
        this.config = config;
        this.registry = registry;
    }

    /**
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        ClientBuilder builder = PulsarClient.builder();
        // 设置Pulsar客户端的配置参数
        if (config != null) {
            // Pulsar的服务地址
            builder.serviceUrl(this.config.getServiceUrl());
            // 认证令牌
            if (StringUtils.isNotBlank(this.config.getAuthentication())) {
                builder.authentication(AuthenticationFactory.token(this.config.getAuthentication()));
            }
            // 默认30秒
            if (this.config.getOperationTimeout() > 0) {
                builder.operationTimeout(this.config.getOperationTimeout(), TimeUnit.SECONDS);
            }
            // 默认50000
            if (this.config.getMaxLookupRequests() > 0) {
                builder.maxLookupRequests(this.config.getMaxLookupRequests());
            }
            // 默认5000
            if (this.config.getMaxConcurrentLookupRequests() > 0) {
                builder.maxConcurrentLookupRequests(this.config.getMaxConcurrentLookupRequests());
            }
            // 单位毫秒
            if (this.config.getConnectionTimeout() > 0) {
                builder.connectionTimeout(this.config.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            }
            // 单位毫秒
            if (this.config.getStartingBackoffInterval() > 0) {
                builder.startingBackoffInterval(this.config.getStartingBackoffInterval(), TimeUnit.MILLISECONDS);
            }
            // 默认30秒
            if (this.config.getKeepAliveInterval() > 0) {
                builder.keepAliveInterval(this.config.getKeepAliveInterval(), TimeUnit.SECONDS);
            }
            // 默认50
            if (this.config.getMaxNumberOfRejectedRequestPerConnection() > 0) {
                builder
                    .maxNumberOfRejectedRequestPerConnection(this.config.getMaxNumberOfRejectedRequestPerConnection());
            }
            // 默认1
            if (this.config.getIoThreads() > 0) {
                builder.ioThreads(this.config.getIoThreads());
            }
            // 默认1
            if (this.config.getListenerThreads() > 0) {
                builder.listenerThreads(this.config.getListenerThreads());
            }
            // 单位毫秒
            if (this.config.getMaxBackoffInterval() > 0) {
                builder.maxBackoffInterval(this.config.getMaxBackoffInterval(), TimeUnit.MILLISECONDS);
            }
            // 默认60秒
            if (this.config.getStatsInterval() > 0) {
                builder.statsInterval(this.config.getStatsInterval(), TimeUnit.SECONDS);
            }
            // 默认false
            builder.allowTlsInsecureConnection(this.config.isAllowTlsInsecureConnection());
            builder.enableTlsHostnameVerification(this.config.isEnableTlsHostnameVerification());
            // 建议设置为false
            builder.enableTcpNoDelay(this.config.isEnableTcpNoDelay());
        }
        this.client = builder.build();
        // 注册消费者
        registry.register(this.client, config);
    }

    /**
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    @Override
    public void destroy() throws Exception {
        Set<Entry<String, PulsarProducer<K, V>>> set = cache.entrySet();
        Iterator<Entry<String, PulsarProducer<K, V>>> it = set.iterator();
        while (it.hasNext()) {
            it.next().getValue().shutdown();
        }

        if (registry != null) {
            registry.shutdown();
        }

        if (this.client != null) {
            this.client.close();
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.MessagingAccessPoint#version()
     */
    @Override
    public String version() {
        return "1.0.0";
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.MessagingAccessPoint#attributes()
     */
    @Override
    public Properties attributes() {
        return properties;
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.MessagingAccessPoint#createProducer(java.lang.String,
     *      java.lang.Object, java.lang.Object, java.util.Properties)
     */
    @Override
    public Producer<K, V> createProducer(String topic, K key, V value, Properties properties) throws Exception {
        PulsarProducer<K, V> producer = cache.get(topic);
        if (producer == null) {
            ProducerTypeEnum type = (ProducerTypeEnum)properties.get(SystemPropKey.PRODUCERTYPE);
            if (type != null) {
                switch (type) {
                    case ASYNC:
                        producer = new AsyncPulsarProducer(this.client, this.config.getPulsarProducerConfig(topic), key,
                            value); // 异步消息生产者
                        break;
                    case DELAYED:
                        producer = new DelayedPulsarProducer(this.client, this.config.getPulsarProducerConfig(topic),
                            key, value); // 延迟消息生产者
                        break;
                    default:
                        producer =
                            new SyncPulsarProducer(this.client, this.config.getPulsarProducerConfig(topic), key, value); // 同步消息生产者
                }
                producer.start();
                cache.put(topic, producer);
            }
        }
        return producer;
    }

}
