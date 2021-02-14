package io.github.storevm.framework.pulsar.config;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2020/07/07
 * @version 1.0.0
 */
@Setter
@Getter
public class PulsarConsumerConfig implements Serializable {
    /**
     * UID
     */
    private static final long serialVersionUID = -2708302984555550434L;

    /**
     * topic
     */
    private String topic;

    /**
     * consumerName
     */
    private String consumerName;

    /**
     * subscriptionName
     */
    private String subscriptionName;

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified.
     */
    private String cryptoFailureAction;

    /**
     * Determines to which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both. Only used
     * with pattern subscriptions.
     */
    private String subscriptionTopicsMode;

    /**
     * Set the {@link SubscriptionInitialPosition} for the consumer.
     */
    private String subscriptionInitialPosition;

    /**
     * Select the subscription type to be used when subscribing to the topic.
     *
     * <p>
     * Options are:
     * <ul>
     * <li>{@link SubscriptionType#Exclusive} (Default)</li>
     * <li>{@link SubscriptionType#Failover}</li>
     * <li>{@link SubscriptionType#Shared}</li>
     * </ul>
     */
    private String subscriptionType;

    /**
     * Specify a pattern for topics that this consumer will subscribe on.
     *
     * <p>
     * The pattern will be applied to subscribe to all the topics, within a single namespace, that will match the
     * pattern.
     *
     * <p>
     * The consumer will automatically subscribe to topics created after itself.
     */
    private String topicsPattern;

    /**
     * Group the consumer acknowledgments for the specified time.
     *
     * <p>
     * By default, the consumer will use a 100 ms grouping time to send out the acknowledgments to the broker.
     *
     * <p>
     * Setting a group time of 0, will send out the acknowledgments immediately. A longer ack group time will be more
     * efficient at the expense of a slight increase in message re-deliveries after a failure.
     */
    private long acknowledgmentGroupTime;

    /**
     * Set the delay to wait before re-delivering messages that have failed to be process.
     *
     * <p>
     * When application uses {@link Consumer#negativeAcknowledge(Message)}, the failed message will be redelivered after
     * a fixed timeout. The default is 1 min.
     */
    private long negativeAckRedeliveryDelay;

    /**
     * Set the timeout for unacked messages, truncated to the nearest millisecond. The timeout needs to be greater than
     * 1 second.
     *
     * <p>
     * By default, the acknowledge timeout is disabled and that means that messages delivered to a consumer will not be
     * re-delivered unless the consumer crashes.
     *
     * <p>
     * When enabling ack timeout, if a message is not acknowledged within the specified timeout it will be re-delivered
     * to the consumer (possibly to a different consumer in case of a shared subscription).
     */
    private long ackTimeout;

    /**
     * Define the granularity of the ack-timeout redelivery.
     *
     * <p>
     * By default, the tick time is set to 1 second. Using an higher tick time will reduce the memory overhead to track
     * messages when the ack-timeout is set to bigger values (eg: 1hour).
     */
    private long ackTimeoutTickTime;

    /**
     * Set dead letter policy for consumer.
     *
     * <p>
     * By default some message will redelivery so many times possible, even to the extent that it can be never stop. By
     * using dead letter mechanism messages will has the max redelivery count, when message exceeding the maximum number
     * of redeliveries, message will send to the Dead Letter Topic and acknowledged automatic.
     *
     * <p>
     * You can enable the dead letter mechanism by setting dead letter policy. example:
     * 
     * <pre>
     * client.newConsumer().deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build()).subscribe();
     * </pre>
     * 
     * Default dead letter topic name is {TopicName}-{Subscription}-DLQ. To setting a custom dead letter topic name
     * 
     * <pre>
     * client.newConsumer().deadLetterPolicy(
     *     DeadLetterPolicy.builder().maxRedeliverCount(10).deadLetterTopic("your-topic-name").build()).subscribe();
     * </pre>
     * 
     * When a dead letter policy is specified, and no ackTimeoutMillis is specified, then the ack timeout will be set to
     * 30000 millisecond.
     */
    private int deadLetterPolicy;

    /**
     * Set the max total receiver queue size across partitons.
     *
     * <p>
     * This setting will be used to reduce the receiver queue size for individual partitions
     * {@link #receiverQueueSize(int)} if the total exceeds this value (default: 50000). The purpose of this setting is
     * to have an upper-limit on the number of messages that a consumer can be pushed at once from a broker, across all
     * the partitions.
     */
    private int maxTotalReceiverQueueSizeAcrossPartitions;

    /**
     * Set topics auto discovery period when using a pattern for topics consumer. The period is in minute, and default
     * and minimum value is 1 minute.
     */
    private int patternAutoDiscoveryPeriod;

    /**
     * <b>Shared subscription</b> Sets priority level for the shared subscription consumers to which broker gives more
     * priority while dispatching messages. Here, broker follows descending priorities. (eg: 0=max-priority, 1, 2,..)
     *
     * <p>
     * In Shared subscription mode, broker will first dispatch messages to max priority-level consumers if they have
     * permits, else broker will consider next priority level consumers.
     *
     * <p>
     * If subscription has consumer-A with priorityLevel 0 and Consumer-B with priorityLevel 1 then broker will dispatch
     * messages to only consumer-A until it runs out permit and then broker starts dispatching messages to Consumer-B.
     *
     * <p>
     * 
     * <pre>
     * Consumer PriorityLevel Permits
     * C1       0             2
     * C2       0             1
     * C3       0             1
     * C4       1             2
     * C5       1             1
     * Order in which broker dispatches messages to consumers: C1, C2, C3, C1, C4, C5, C4
     * </pre>
     *
     * <p>
     * <b>Failover subscription</b> Broker selects active consumer for a failover-subscription based on consumer's
     * priority-level and lexicographical sorting of a consumer name. eg:
     * 
     * <pre>
     * 1. Active consumer = C1 : Same priority-level and lexicographical sorting
     * Consumer PriorityLevel Name
     * C1       0             aaa
     * C2       0             bbb
     *
     * 2. Active consumer = C2 : Consumer with highest priority
     * Consumer PriorityLevel Name
     * C1       1             aaa
     * C2       0             bbb
     *
     * Partitioned-topics:
     * Broker evenly assigns partitioned topics to highest priority consumers.
     * </pre>
     */
    private int priorityLevel;

    /**
     * Sets the size of the consumer receive queue.
     *
     * <p>
     * The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     *
     * <p>
     * <b>Setting the consumer queue size as zero</b>
     * <ul>
     * <li>Decreases the throughput of the consumer, by disabling pre-fetching of messages. This approach improves the
     * message distribution on shared subscription, by pushing messages only to the consumers that are ready to process
     * them. Neither {@link Consumer#receive(int, TimeUnit)} nor Partitioned Topics can be used if the consumer queue
     * size is zero. {@link Consumer#receive()} function call should not be interrupted when the consumer queue size is
     * zero.</li>
     * <li>Doesn't support Batch-Message: if consumer receives any batch-message then it closes consumer connection with
     * broker and {@link Consumer#receive()} call will remain blocked while {@link Consumer#receiveAsync()} receives
     * exception in callback. <b> consumer will not be able receive any further message unless batch-message in pipeline
     * is removed</b></li>
     * </ul>
     * Default value is {@code 1000} messages and should be good for most use cases.
     */
    private int receiverQueueSize;

    /**
     * If enabled, the consumer will auto subscribe for partitions increasement. This is only for partitioned consumer.
     */
    private boolean autoUpdatePartitions;

    /**
     * If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog
     * of the topic. This means that, if the topic has been compacted, the consumer will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages will be sent as normal.
     *
     * <p>
     * readCompacted can only be enabled subscriptions to persistent topics, which have a single active consumer (i.e.
     * failure or exclusive subscriptions). Attempting to enable it on subscriptions to a non-persistent topics or on a
     * shared subscription, will lead to the subscription call throwing a PulsarClientException.
     */
    private boolean readCompacted;

    /**
     * replicateSubscriptionState
     */
    private boolean replicateSubscriptionState;

    /**
     * 消費者訂閲消息的類型，默認為字節數組
     */
    private Class<?> schemaClass = byte[].class;

    /**
     * 消费者订阅消息的键值类型，默认是字符串
     */
    private Class<?> schemaKeyClass = byte[].class;
}
