package io.github.storevm.framework.pulsar.consumer.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.InitializingBean;

import io.github.storevm.framework.pulsar.MessageManagement;
import io.github.storevm.framework.pulsar.config.PulsarConsumerConfig;
import io.github.storevm.framework.pulsar.consumer.MessageHandler;
import io.github.storevm.framework.pulsar.model.Action;
import io.github.storevm.framework.pulsar.model.PulsarMessage;
import io.github.storevm.framework.pulsar.model.RecordSchema;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Slf4j
public class PulsarConsumer<T> extends MessageManagement
    implements io.github.storevm.framework.pulsar.consumer.Consumer, InitializingBean {
    private PulsarClient client; // Pulsar client instance
    private Consumer<T> consumer; // Pulsar consumer instance
    private PulsarConsumerConfig config; // Pulsar consumer configuration
    private int concurrency = 1;
    private MessageHandler listener;
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
        ConsumerBuilder builder = this.client.newConsumer(new RecordSchema(this.config.getSchemaClass()).getSchema());
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
     * @see io.github.storevm.framework.pulsar.MessageManagement#doStart()
     */
    @Override
    protected void doStart() {
        io.execute(new Runnable() {
            @Override
            public void run() {
                subscribe();
            }
        });
    }

    /**
     * @see io.github.storevm.framework.pulsar.MessageManagement#doClose()
     */
    @Override
    protected void doClose() {
        if (this.consumer != null) {
            try {
                this.consumer.close();
                shutdownExecutors(io, worker);
            } catch (PulsarClientException ex) {
                log.error("关闭Pulsar消费者时发生异常", ex);
                throw new RuntimeException("关闭Pulsar消费者时发生异常");
            }
        }
    }

    /**
     * @see io.github.storevm.framework.pulsar.consumer.Consumer#addListener(io.github.storevm.framework.pulsar.consumer.MessageHandler,
     *      java.lang.String[])
     */
    @Override
    public void addListener(MessageHandler listener, String... topics) {
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
