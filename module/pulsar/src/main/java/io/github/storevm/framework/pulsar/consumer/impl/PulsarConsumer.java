package io.github.storevm.framework.pulsar.consumer.impl;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.springframework.beans.factory.InitializingBean;

import io.github.storevm.framework.pulsar.config.PulsarConsumerConfig;
import io.github.storevm.framework.pulsar.consumer.MessageListener;
import io.github.storevm.framework.pulsar.model.Action;
import io.github.storevm.framework.pulsar.model.PulsarMessage;
import io.github.storevm.framework.pulsar.model.ServiceLifeState;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Slf4j
public class PulsarConsumer<T> implements io.github.storevm.framework.pulsar.consumer.Consumer, InitializingBean {
    private transient ServiceLifeState status = ServiceLifeState.INITIALIZED; // status
    private PulsarClient client; // Pulsar client instance
    private Consumer<T> consumer; // Pulsar consumer instance
    private PulsarConsumerConfig config; // Pulsar consumer configuration
    private int concurrency = 1;
    private MessageListener listener;
    private ExecutorService io;
    private ExecutorService worker;

    /**
     * constructor
     * 
     * @param client
     * @param config
     */
    public PulsarConsumer(PulsarClient client, PulsarConsumerConfig config) {
        this.client = client;
        this.config = config;
    }

    /**
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        ConsumerBuilder builder = this.client.newConsumer(toSchema(this.config.getSchemaClass()));
        // 进行消费者对象的参数设置
        builder.topic(this.config.getTopic());
        builder.subscriptionName(StringUtils.isNotBlank(this.config.getSubscriptionName())
            ? this.config.getSubscriptionName() : this.config.getConsumerName());
        ConsumerCryptoFailureAction ccf =
            EnumUtils.getEnum(ConsumerCryptoFailureAction.class, this.config.getCryptoFailureAction());
        if (StringUtils.isNotBlank(this.config.getConsumerName())) {
            builder.consumerName(this.config.getConsumerName());
        }
        if (ccf != null) {
            builder.cryptoFailureAction(ccf);
        }
        RegexSubscriptionMode rsm =
            EnumUtils.getEnum(RegexSubscriptionMode.class, this.config.getSubscriptionTopicsMode());
        if (rsm != null) {
            builder.subscriptionTopicsMode(rsm);
        }
        SubscriptionInitialPosition sip =
            EnumUtils.getEnum(SubscriptionInitialPosition.class, this.config.getSubscriptionInitialPosition());
        if (sip != null) {
            builder.subscriptionInitialPosition(sip);
        }
        SubscriptionType st = EnumUtils.getEnum(SubscriptionType.class, this.config.getSubscriptionType());
        if (st != null) {
            builder.subscriptionType(st);
        }
        if (StringUtils.isNotBlank(this.config.getTopicsPattern())) {
            builder.topicsPattern(Pattern.compile(this.config.getTopicsPattern()));
        }
        // 默认100毫秒
        if (this.config.getAcknowledgmentGroupTime() > 0) {
            builder.acknowledgmentGroupTime(this.config.getAcknowledgmentGroupTime(), TimeUnit.MILLISECONDS);
        }
        // 默认60秒
        if (this.config.getNegativeAckRedeliveryDelay() > 0) {
            builder.negativeAckRedeliveryDelay(this.config.getNegativeAckRedeliveryDelay(), TimeUnit.SECONDS);
        }
        // 大于1秒
        if (this.config.getAckTimeout() > 0) {
            builder.ackTimeout(this.config.getAckTimeout(), TimeUnit.SECONDS);
        }
        // 默认1秒
        if (this.config.getAckTimeoutTickTime() > 0) {
            builder.ackTimeoutTickTime(this.config.getAckTimeoutTickTime(), TimeUnit.SECONDS);
        }
        // 10
        if (this.config.getDeadLetterPolicy() > 0) {
            builder.deadLetterPolicy(
                DeadLetterPolicy.builder().maxRedeliverCount(this.config.getDeadLetterPolicy()).build());
        }
        if (this.config.getMaxTotalReceiverQueueSizeAcrossPartitions() > 0) {
            builder
                .maxTotalReceiverQueueSizeAcrossPartitions(this.config.getMaxTotalReceiverQueueSizeAcrossPartitions());
        }
        if (this.config.getPatternAutoDiscoveryPeriod() > 0) {
            builder.patternAutoDiscoveryPeriod(this.config.getPatternAutoDiscoveryPeriod());
        }
        if (this.config.getPriorityLevel() > 0) {
            builder.priorityLevel(this.config.getPriorityLevel());
        }
        if (this.config.getReceiverQueueSize() > 0) {
            builder.receiverQueueSize(this.config.getReceiverQueueSize());
        }
        builder.autoUpdatePartitions(this.config.isAutoUpdatePartitions());
        builder.readCompacted(this.config.isReadCompacted());
        builder.replicateSubscriptionState(this.config.isReplicateSubscriptionState());
        // 初始化綫程池
        io = Executors.newSingleThreadExecutor();
        worker = Executors.newFixedThreadPool(concurrency);
        // 启动消费者订阅
        this.consumer = builder.subscribe();
    }

    /**
     * 
     */
    @Override
    public boolean isStarted() {
        return status == ServiceLifeState.STARTED;
    }

    /**
     * 
     * @return
     */
    @Override
    public boolean isClosed() {
        return status == ServiceLifeState.STOPPED;
    }

    /**
     * 
     */
    @Override
    public void start() {
        if (!isStarted()) {
            io.execute(new Runnable() {
                @Override
                public void run() {
                    subscribe();
                }
            });
            status = ServiceLifeState.STARTED;
        }
    }

    /**
     * 
     */
    @Override
    public void shutdown() {
        if (this.consumer != null) {
            try {
                this.consumer.close();
                shutdownExecutors(io, worker);
                this.status = ServiceLifeState.STOPPED;
            } catch (PulsarClientException ex) {
                log.error("关闭Pulsar消费者时发生异常", ex);
                throw new RuntimeException("关闭Pulsar消费者时发生异常");
            }
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.consumer.Consumer#addListener(io.github.storevm.framework.pulsar.consumer.MessageListener,
     *      java.lang.String[])
     */
    @Override
    public void addListener(MessageListener listener, String... topics) {
        if (ArrayUtils.contains(topics, this.config.getTopic())) {
            this.listener = listener;
            // 启动消费者
            if (!isStarted()) {
                start();
            }
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.consumer.Consumer#subscribe()
     */
    @Override
    public void subscribe() {
        while (this.consumer.isConnected()) {
            try {
                // 异步接收消息
                CompletableFuture<org.apache.pulsar.client.api.Message<T>> future = this.consumer.receiveAsync();
                final org.apache.pulsar.client.api.Message<T> message = future.get();
                if (message != null) {
                    if (this.concurrency > 1) {
                        this.worker.execute(new Runnable() {
                            @Override
                            public void run() {
                                processMessage(message);
                            }
                        });
                    } else {
                        processMessage(message);
                    }
                }
            } catch (ExecutionException ex) {
            } catch (Exception ex) {
                log.error("接收Pulsar消息时发生系统异常", ex);
            }
        }
    }

    /**
     * 
     * @param concurrency
     */
    @Override
    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    /**
     * 根据类型转换成Pulsar指定类型的schema
     * 
     * @param cls
     * @return
     */
    private Schema<?> toSchema(Class<?> cls) {
        if (ClassUtils.isAssignable(cls, byte[].class) || ClassUtils.isAssignable(cls, ByteBuffer.class)
            || ClassUtils.isAssignable(cls, ByteBuf.class)) {
            return Schema.BYTES;
        } else if (ClassUtils.isAssignable(cls, String.class)) {
            return Schema.STRING;
        } else if (ClassUtils.isAssignable(cls, Integer.class)) {
            return Schema.INT32;
        } else if (ClassUtils.isAssignable(cls, Long.class)) {
            return Schema.INT64;
        } else if (ClassUtils.isAssignable(cls, Short.class)) {
            return Schema.INT16;
        } else if (ClassUtils.isAssignable(cls, Byte.class)) {
            return Schema.INT8;
        } else if (ClassUtils.isAssignable(cls, Float.class)) {
            return Schema.FLOAT;
        } else if (ClassUtils.isAssignable(cls, Double.class)) {
            return Schema.DOUBLE;
        } else if (ClassUtils.isAssignable(cls, Boolean.class)) {
            return Schema.BOOL;
        } else if (ClassUtils.isAssignable(cls, Date.class)) {
            return Schema.DATE;
        } else if (ClassUtils.isAssignable(cls, Timestamp.class)) {
            return Schema.TIMESTAMP;
        } else if (ClassUtils.isAssignable(cls, Time.class)) {
            return Schema.TIME;
        } else {
            return toGenericSchema(cls);
        }
    }

    /**
     * 转换成泛型的schema
     * 
     * @param cls
     * @return
     */
    private GenericSchema<GenericRecord> toGenericSchema(Class<?> cls) {
        Field[] fields = FieldUtils.getAllFields(cls);
        RecordSchemaBuilder builder = SchemaBuilder.record(ClassUtils.getShortClassName(cls));
        for (int i = 0, n = fields.length; i < n; i++) {
            if (!StringUtils.equalsIgnoreCase("serialVersionUID", fields[i].getName())) {
                if (ClassUtils.isAssignable(fields[i].getType(), Boolean.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.BOOLEAN);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Byte.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT8);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Short.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT16);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Integer.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT32);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Long.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT64);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Float.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.FLOAT);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Double.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.DOUBLE);
                } else if (ClassUtils.isAssignable(fields[i].getType(), byte[].class)
                    || ClassUtils.isAssignable(fields[i].getType(), ByteBuffer.class)
                    || ClassUtils.isAssignable(fields[i].getType(), ByteBuf.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.BYTES);
                } else if (ClassUtils.isAssignable(fields[i].getType(), String.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.STRING);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Timestamp.class)
                    || ClassUtils.isAssignable(fields[i].getType(), Date.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.TIMESTAMP);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Time.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.TIME);
                } else if (ClassUtils.isAssignable(fields[i].getType(), BigDecimal.class)
                    || ClassUtils.isAssignable(fields[i].getType(), BigInteger.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.STRING);
                } else {
                    // 复杂对象
                    GenericSchema<GenericRecord> s = toGenericSchema(fields[i].getType());
                    builder.field(fields[i].getName(), s).type(SchemaType.AVRO);
                }
            }
        }
        SchemaInfo schemaInfo = builder.build(SchemaType.AVRO);
        return Schema.generic(schemaInfo);
    }

    /**
     * 关闭线程池
     * 
     * @param services
     */
    private void shutdownExecutors(ExecutorService... services) {
        if (services != null) {
            for (ExecutorService service : services) {
                service.shutdown();
                try {
                    // Wait a while for existing tasks to terminate
                    if (!service.awaitTermination(60, TimeUnit.SECONDS)) {
                        service.shutdownNow(); // Cancel currently executing tasks
                        // Wait a while for tasks to respond to being cancelled
                        if (!service.awaitTermination(60, TimeUnit.SECONDS))
                            log.warn("Pool did not terminate");
                    }
                } catch (InterruptedException ie) {
                    // (Re-)Cancel if current thread also interrupted
                    service.shutdownNow();
                    // Preserve interrupt status
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 处理Pulsar消息
     * 
     * @param message
     */
    private void processMessage(org.apache.pulsar.client.api.Message message) {
        Action action = listener.consume(new PulsarMessage(message));
        if (action == Action.CommitMessage) {
            // 提交
            try {
                consumer.acknowledge(message.getMessageId());
            } catch (PulsarClientException ex) {
                log.error("提交Pulsar消息时发生系统异常", ex);
            }
        }
    }
}
