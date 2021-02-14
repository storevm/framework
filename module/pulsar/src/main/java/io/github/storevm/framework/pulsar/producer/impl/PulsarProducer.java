package io.github.storevm.framework.pulsar.producer.impl;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.springframework.beans.factory.InitializingBean;

import io.github.storevm.framework.pulsar.config.PulsarProducerConfig;
import io.github.storevm.framework.pulsar.exception.OnExceptionContext;
import io.github.storevm.framework.pulsar.model.GenericSchemaMetadata;
import io.github.storevm.framework.pulsar.model.Message;
import io.github.storevm.framework.pulsar.model.ProducerRecord;
import io.github.storevm.framework.pulsar.model.PulsarMessage;
import io.github.storevm.framework.pulsar.model.PulsarSendResult;
import io.github.storevm.framework.pulsar.model.RecordMetadata;
import io.github.storevm.framework.pulsar.model.SendResult;
import io.github.storevm.framework.pulsar.model.ServiceLifeState;
import io.github.storevm.framework.pulsar.model.TopicPartition;
import io.github.storevm.framework.pulsar.producer.MessageBuilder;
import io.github.storevm.framework.pulsar.producer.SendCallback;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Slf4j
public class PulsarProducer<K, V>
    implements io.github.storevm.framework.pulsar.producer.Producer<K, V>, InitializingBean {
    private transient ServiceLifeState status = ServiceLifeState.INITIALIZED; // status
    private PulsarClient client; // Pulsar client instance
    private PulsarProducerConfig config; // Pulsar producer configuration
    private Producer<V> producer; // Pulsar producer instance
    private Schema<?> schema; // 消息的schema
    private GenericSchemaMetadata metadata = new GenericSchemaMetadata();

    /**
     * constructor
     * 
     * @param client
     * @param config
     */
    public PulsarProducer(PulsarClient client, PulsarProducerConfig config) {
        this.client = client;
        this.config = config;
    }

    /**
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        if (producer == null && schema != null && this.status != ServiceLifeState.STARTED) {
            ProducerBuilder builder = client.newProducer(schema);
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
                MessageRoutingMode mrm =
                    EnumUtils.getEnum(MessageRoutingMode.class, this.config.getMessageRoutingMode());
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
        this.status = ServiceLifeState.STARTED;
    }

    /**
     * 
     */
    @Override
    public void shutdown() {
        if (this.producer != null) {
            try {
                this.producer.close();
                this.status = ServiceLifeState.STOPPED;
            } catch (PulsarClientException ex) {
                log.error("关闭Pulsar生产者时发生异常", ex);
                throw new RuntimeException("关闭Pulsar生产者时发生异常");
            }
        }
    }

    /**
     * 
     * @param value
     */
    public void setSchema(V value) {
        this.schema = toSchema(value);
        if (this.schema == null) {
            // 设置泛型的schema
            this.schema = toGenericSchema(value, this.metadata);
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
     * @see io.github.storevm.framework.pulsar.producer.Producer#send(io.github.storevm.framework.pulsar.model.Message)
     */
    @Override
    public SendResult send(Message message) {
        if (message != null && (message instanceof PulsarMessage)) {
            // 转换消息类型
            PulsarMessage<K, V> pm = (PulsarMessage<K, V>)message;
            if (producer != null) {
                try {
                    TypedMessageBuilder builder = toMessageBuilder(pm);
                    // 同步发送
                    MessageId id = builder.send();
                    return toSendResult(pm, id);
                } catch (PulsarClientException ex) {
                    log.error("同步发送Pulsar消息时发生异常", ex);
                    throw new RuntimeException("同步发送Pulsar消息时发生异常");
                }
            }
        }
        return null;
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.Producer#sendAsync(io.github.storevm.framework.pulsar.model.Message,
     *      io.github.storevm.framework.pulsar.producer.SendCallback)
     */
    @Override
    public void sendAsync(Message message, SendCallback sendCallback) {
        if (message != null && (message instanceof PulsarMessage)) {
            // 转换消息类型
            PulsarMessage<K, V> pm = (PulsarMessage<K, V>)message;
            if (producer != null) {
                TypedMessageBuilder builder = toMessageBuilder(pm);
                // 发送异步消息
                CompletableFuture<MessageId> future = builder.sendAsync();
                future.thenAccept(id -> {
                    SendResult<K, V> result = toSendResult(pm, id);
                    sendCallback.onSuccess(result);
                }).exceptionally(ex -> {
                    OnExceptionContext context = new OnExceptionContext();
                    context.setException(new RuntimeException(ex));
                    context.setTopic(message.getTopic());
                    sendCallback.onException(context);
                    return null;
                });
            }
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.Producer#delayedDelivery(io.github.storevm.framework.pulsar.model.Message,
     *      long)
     */
    @Override
    public SendResult delayedDelivery(Message message, long delay) {
        if (message != null && (message instanceof PulsarMessage)) {
            // 转换消息类型
            PulsarMessage<K, V> pm = (PulsarMessage<K, V>)message;
            if (producer != null) {
                try {
                    TypedMessageBuilder builder = toMessageBuilder(pm);
                    // 设置延迟时间(单位：毫秒)
                    builder.deliverAfter(delay, TimeUnit.MILLISECONDS);
                    // 同步发送
                    MessageId id = builder.send();
                    return toSendResult(pm, id);
                } catch (PulsarClientException ex) {
                    log.error("同步发送Pulsar延迟消息时发生异常", ex);
                    throw new RuntimeException("同步发送Pulsar延迟消息时发生异常");
                }
            }
        }
        return null;
    }

    private Schema<?> toSchema(V value) {
        if (value != null) {
            if (value instanceof byte[] || value instanceof ByteBuffer || value instanceof ByteBuf) {
                return Schema.BYTES;
            } else if (value instanceof String) {
                return Schema.STRING;
            } else if (value instanceof Integer) {
                return Schema.INT32;
            } else if (value instanceof Long) {
                return Schema.INT64;
            } else if (value instanceof Short) {
                return Schema.INT16;
            } else if (value instanceof Byte) {
                return Schema.INT8;
            } else if (value instanceof Float) {
                return Schema.FLOAT;
            } else if (value instanceof Double) {
                return Schema.DOUBLE;
            } else if (value instanceof Boolean) {
                return Schema.BOOL;
            } else if (value instanceof Date) {
                return Schema.DATE;
            } else if (value instanceof Timestamp) {
                return Schema.TIMESTAMP;
            } else if (value instanceof Time) {
                return Schema.TIME;
            }
        }
        return null;
    }

    private GenericSchema<GenericRecord> toGenericSchema(V value, GenericSchemaMetadata metadata) {
        Field[] fields = FieldUtils.getAllFields(value.getClass());
        RecordSchemaBuilder builder = SchemaBuilder.record(ClassUtils.getShortClassName(value.getClass()));
        for (int i = 0, n = fields.length; i < n; i++) {
            if (!StringUtils.equalsIgnoreCase("serialVersionUID", fields[i].getName())) {
                if (ClassUtils.isAssignable(fields[i].getType(), Boolean.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.BOOLEAN);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Byte.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT8);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Short.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT16);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Integer.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT32);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Long.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT64);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Float.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.FLOAT);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Double.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.DOUBLE);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), byte[].class)
                    || ClassUtils.isAssignable(fields[i].getType(), ByteBuffer.class)
                    || ClassUtils.isAssignable(fields[i].getType(), ByteBuf.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.BYTES);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), String.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.STRING);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Timestamp.class)
                    || ClassUtils.isAssignable(fields[i].getType(), Date.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.TIMESTAMP);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Time.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.TIME);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), BigDecimal.class)
                    || ClassUtils.isAssignable(fields[i].getType(), BigInteger.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.STRING);
                    metadata.addChild(fields[i], null);
                } else {
                    try {
                        // 复杂对象
                        V subo = (V)FieldUtils.readField(fields[i], value, true);
                        GenericSchemaMetadata m = new GenericSchemaMetadata();
                        GenericSchema<GenericRecord> s = toGenericSchema(subo, m);
                        builder.field(fields[i].getName(), s).type(SchemaType.AVRO);
                        m.setSchema(s);
                        metadata.addChild(fields[i], m);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException("创建GenericSchema时发生异常");
                    }
                }
            }
        }
        SchemaInfo schemaInfo = builder.build(SchemaType.AVRO);
        return Schema.generic(schemaInfo);
    }

    private TypedMessageBuilder toMessageBuilder(PulsarMessage<K, V> pm) {
        try {
            TypedMessageBuilder builder = this.producer.newMessage();
            if (pm.getKey() != null) {
                builder.key(String.valueOf(pm.getKey())); // 设置消息的键值
            }
            if (pm.getUserProperties() != null) {
                builder.properties(pm.getUserProperties()); // 设置属性
            }
            if (schema instanceof GenericSchema) {
                // 泛型设置
                builder.value(
                    GenericRecordCreator.builder(metadata).create(pm.getValue(), (GenericSchema<GenericRecord>)schema));
            } else {
                builder.value(pm.getValue());
            }
            return builder;
        } catch (IllegalAccessException ex) {
            log.error("同步发送Pulsar消息时发生异常", ex);
            throw new RuntimeException("同步发送Pulsar消息时发生异常");
        }
    }

    private SendResult<K, V> toSendResult(PulsarMessage message, MessageId id) {
        ProducerRecord<K, V> record =
            new ProducerRecord(message.getTopic(), (K)message.getKey(), (V)message.getValue());
        io.github.storevm.framework.pulsar.model.TopicPartition tp = message.getTopicPartition();
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(tp.topic(), tp.partition()),
            message.getOffset(), message.getBornTimestamp(), -1L, -1, -1);
        SendResult<K, V> result = new PulsarSendResult(record, metadata, id);
        return result;
    }
}
