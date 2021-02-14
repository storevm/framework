package io.github.storevm.framework.pulsar.producer.impl;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;

import io.github.storevm.framework.pulsar.MessageManagement;
import io.github.storevm.framework.pulsar.config.PulsarProducerConfig;
import io.github.storevm.framework.pulsar.model.ProducerRecord;
import io.github.storevm.framework.pulsar.model.PulsarMessage;
import io.github.storevm.framework.pulsar.model.PulsarSendResult;
import io.github.storevm.framework.pulsar.model.RecordMetadata;
import io.github.storevm.framework.pulsar.model.RecordSchema;
import io.github.storevm.framework.pulsar.model.SendResult;
import io.github.storevm.framework.pulsar.model.TopicPartition;
import io.github.storevm.framework.pulsar.producer.MessageBuilder;
import io.github.storevm.framework.pulsar.producer.SendCallback;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Slf4j
public abstract class PulsarProducer<K, V> extends MessageManagement
    implements io.github.storevm.framework.pulsar.producer.Producer<K, V> {
    private final PulsarClient client; // Pulsar client instance
    private final PulsarProducerConfig config; // Pulsar producer configuration
    protected Producer<V> producer; // Pulsar producer instance
    private RecordSchema<K, V> record; // record schema

    /**
     * constructor
     * 
     * @param client
     * @param config
     * @param key
     * @param value
     */
    public PulsarProducer(final PulsarClient client, final PulsarProducerConfig config, final K key, final V value) {
        this.client = client;
        this.config = config;
        this.record = new RecordSchema(key, value);
    }

    /**
     * @see io.github.storevm.framework.pulsar.MessageManagement#doStart()
     */
    @Override
    protected void doStart() {
        ProducerBuilder builder = client.newProducer(this.record.getSchema());
        if (this.config != null) {
            builder.topic(this.config.getTopic());
            ProducerCryptoFailureAction pcfa =
                EnumUtils.getEnum(ProducerCryptoFailureAction.class, this.config.getCryptoFailureAction());
            if (StringUtils.isNotBlank(this.config.getProducerName())) {
                builder.producerName(this.config.getProducerName());
            }
            if (pcfa != null) {
                builder.cryptoFailureAction(pcfa);
            }
            CompressionType ct = EnumUtils.getEnum(CompressionType.class, this.config.getCompressionType());
            if (ct != null) {
                builder.compressionType(ct);
            }
            HashingScheme hs = EnumUtils.getEnum(HashingScheme.class, this.config.getHashingScheme());
            if (hs != null) {
                builder.hashingScheme(hs);
            }
            MessageRoutingMode mrm = EnumUtils.getEnum(MessageRoutingMode.class, this.config.getMessageRoutingMode());
            if (mrm != null) {
                builder.messageRoutingMode(mrm);
            }
            // 默认1000
            if (this.config.getBatchingMaxMessages() > 0) {
                builder.batchingMaxMessages(this.config.getBatchingMaxMessages());
            }
            // 默认1000
            if (this.config.getMaxPendingMessages() > 0) {
                builder.maxPendingMessages(this.config.getMaxPendingMessages());
            }
            // 默认50000
            if (this.config.getMaxPendingMessagesAcrossPartitions() > 0) {
                builder.maxPendingMessagesAcrossPartitions(this.config.getMaxPendingMessagesAcrossPartitions());
            }
            // 默认1ms
            if (this.config.getBatchingMaxPublishDelay() > 0) {
                builder.batchingMaxPublishDelay(this.config.getBatchingMaxPublishDelay(), TimeUnit.MILLISECONDS);
            }
            // 默认30秒
            if (this.config.getSendTimeout() > 0) {
                builder.sendTimeout(this.config.getSendTimeout(), TimeUnit.SECONDS);
            }
            // 默认true
            builder.enableBatching(this.config.isEnableBatching());
            builder.blockIfQueueFull(this.config.isBlockIfQueueFull());
            try {
                this.producer = builder.create();
            } catch (PulsarClientException ex) {
                log.error("启动Pulsar生产者时发生异常", ex);
                throw new RuntimeException("启动Pulsar生产者时发生异常");
            }
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.MessageManagement#doClose()
     */
    @Override
    protected void doClose() {
        try {
            this.producer.close();
        } catch (PulsarClientException ex) {
            log.error("关闭Pulsar生产者时发生异常", ex);
            throw new RuntimeException("关闭Pulsar生产者时发生异常");
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.ProducerBase#messageBuilder()
     */
    @Override
    public MessageBuilder<K, V> messageBuilder() {
        return new PulsarMessageBuilder();
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.Producer#setCallback(io.github.storevm.framework.pulsar.producer.SendCallback)
     */
    @Override
    public void setCallback(SendCallback<K, V> callback) {
        throw new UnsupportedOperationException(); // 不持支的操作
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.Producer#setDelay(long)
     */
    @Override
    public void setDelay(long delay) {
        throw new UnsupportedOperationException(); // 不持支的操作
    }

    protected TypedMessageBuilder toMessageBuilder(PulsarMessage<K, V> pm) {
        try {
            TypedMessageBuilder builder = this.producer.newMessage();
            if (pm.getKey() != null) {
                builder.key(String.valueOf(pm.getKey())); // 设置消息的键值
            }
            if (pm.getUserProperties() != null) {
                builder.properties(pm.getUserProperties()); // 设置属性
            }
            if (this.record.getSchema() instanceof GenericSchema) {
                // 泛型设置
                builder.value(GenericRecordCreator.newInstance(this.record).create(pm.getValue()));
            } else {
                builder.value(pm.getValue());
            }
            return builder;
        } catch (IllegalAccessException ex) {
            log.error("同步发送Pulsar消息时发生异常", ex);
            throw new RuntimeException("同步发送Pulsar消息时发生异常");
        }
    }

    protected SendResult<K, V> toSendResult(PulsarMessage message, MessageId id) {
        ProducerRecord<K, V> record =
            new ProducerRecord(message.getTopic(), (K)message.getKey(), (V)message.getValue());
        io.github.storevm.framework.pulsar.model.TopicPartition tp = message.getTopicPartition();
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(tp.topic(), tp.partition()),
            message.getOffset(), message.getBornTimestamp(), -1L, -1, -1);
        SendResult<K, V> result = new PulsarSendResult(record, metadata, id);
        return result;
    }
}
