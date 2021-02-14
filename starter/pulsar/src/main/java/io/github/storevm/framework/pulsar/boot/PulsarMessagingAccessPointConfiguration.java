package io.github.storevm.framework.pulsar.boot;

import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.storevm.framework.pulsar.consumer.impl.PulsarConsumerRegistry;
import io.github.storevm.framework.pulsar.producer.MessagingAccessPoint;
import io.github.storevm.framework.pulsar.producer.impl.PulsarMessagingAccessPoint;

/**
 * @author Jack
 * @date 2020/07/02
 * @version 1.0.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(PulsarClient.class)
public class PulsarMessagingAccessPointConfiguration extends MessagingAccessPointConfiguration {
    /**
     * constructor
     * 
     * @param properties
     */
    @Autowired
    public PulsarMessagingAccessPointConfiguration(PulsarConfigProperties properties) {
        super(properties);
    }

    @Bean
    public MessagingAccessPoint messagingAccessPoint(PulsarConsumerRegistry registry) {
        return new PulsarMessagingAccessPoint(getProperties().parseConfig(), registry);
    }

    @Bean
    public PulsarConsumerRegistry pulsarConsumerRegistry() {
        return new PulsarConsumerRegistry();
    }
}
