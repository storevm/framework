package io.github.storevm.framework.pulsar.boot;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.github.storevm.framework.pulsar.ProducerTemplate;
import io.github.storevm.framework.pulsar.producer.MessagingAccessPoint;

/**
 * @author Jack
 * @date 2020/07/02
 * @version 1.0.0
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PulsarConfigProperties.class)
@Import({PulsarMessagingAccessPointConfiguration.class})
public class PulsarAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ProducerTemplate producerTemplate(MessagingAccessPoint point) {
        ProducerTemplate template = new ProducerTemplate(point);
        return template;
    }
}
