package io.github.storevm.framework.pulsar.boot;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;

import io.github.storevm.framework.pulsar.config.PulsarClientConfig;
import io.github.storevm.framework.pulsar.config.PulsarConsumerConfig;
import io.github.storevm.framework.pulsar.config.PulsarProducerConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2020/07/02
 * @version 1.0.0
 */
@Setter
@Getter
@ConfigurationProperties(prefix = "spring")
public class PulsarConfigProperties implements Serializable {
    /**
     * UID
     */
    private static final long serialVersionUID = 7935424937976380365L;

    /**
     * pulsar config
     */
    private PulsarMessagingConfigProperties pulsar;

    /**
     * 转换配置参数对象
     * 
     * @return
     */
    public PulsarClientConfig parseConfig() {
        PulsarClientConfig config = new PulsarClientConfig();
        BeanUtils.copyProperties(pulsar, config, "producers", "consumers");
        parseProducersConfig(config);
        parseConsumersConfig(config);
        return config;
    }

    /**
     * 解析生产者配置参数
     * 
     * @param config
     */
    private void parseProducersConfig(PulsarClientConfig config) {
        Set<Entry<String, PulsarProducerConfigProperties>> set = pulsar.getProducers().entrySet();
        Iterator<Entry<String, PulsarProducerConfigProperties>> it = set.iterator();
        while (it.hasNext()) {
            Entry<String, PulsarProducerConfigProperties> entry = it.next();
            PulsarProducerConfig ppc = new PulsarProducerConfig();
            BeanUtils.copyProperties(entry.getValue(), ppc);
            config.getProducers().put(entry.getKey(), ppc);
        }
    }

    /**
     * 解析消费者配置信息
     * 
     * @param config
     */
    private void parseConsumersConfig(PulsarClientConfig config) {
        Set<Entry<String, PulsarConsumerConfigProperties>> set = pulsar.getConsumers().entrySet();
        Iterator<Entry<String, PulsarConsumerConfigProperties>> it = set.iterator();
        while (it.hasNext()) {
            Entry<String, PulsarConsumerConfigProperties> entry = it.next();
            PulsarConsumerConfig pccs = new PulsarConsumerConfig();
            BeanUtils.copyProperties(entry.getValue(), pccs);
            config.getConsumers().put(entry.getKey(), pccs);
        }
    }
}
