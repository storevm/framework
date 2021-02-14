package io.github.storevm.framework.pulsar.boot;

import java.io.Serializable;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException.ProducerQueueIsFullError;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2020/07/06
 * @version 1.0.0
 */
@Setter
@Getter
public class PulsarProducerConfigProperties implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -687067918082912874L;

    /**
     * topic
     */
    private String topic;

    /**
     * producerName
     */
    private String producerName;

    /**
     * NONE,LZ4,ZLIB,ZSTD,SNAPPY
     */
    private String compressionType;

    /**
     * Sets the ProducerCryptoFailureAction to the value specified. FAIL,SEND
     */
    private String cryptoFailureAction;

    /**
     * JavaStringHash,Murmur3_32Hash
     */
    private String hashingScheme;

    /**
     * SinglePartition,RoundRobinPartition,CustomPartition
     */
    private String messageRoutingMode;

    /**
     * Set the maximum number of messages permitted in a batch. <i>default: 1000</i> If set to a value greater than 1,
     * messages will be queued until this threshold is reached or batch interval has elapsed.
     *
     * <p>
     * All messages in batch will be published as a single batch message. The consumer will be delivered individual
     * messages in the batch in the same order they were enqueued.
     */
    private int batchingMaxMessages;

    /**
     * Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
     *
     * <p>
     * When the queue is full, by default, all calls to {@link Producer#send} and {@link Producer#sendAsync} will fail
     * unless {@code blockIfQueueFull=true}. Use {@link #blockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * <p>
     * The producer queue size also determines the max amount of memory that will be required by the client application.
     * Until, the producer gets a successful acknowledgment back from the broker, it will keep in memory (direct memory
     * pool) all the messages in the pending queue.
     *
     * <p>
     * Default is 1000.
     */
    private int maxPendingMessages;

    /**
     * Set the number of max pending messages across all the partitions.
     *
     * <p>
     * This setting will be used to lower the max pending messages for each partition
     * ({@link #maxPendingMessages(int)}), if the total exceeds the configured value. The purpose of this setting is to
     * have an upper-limit on the number of pending messages when publishing on a partitioned topic.
     *
     * <p>
     * Default is 50000.
     *
     * <p>
     * If publishing at high rate over a topic with many partitions (especially when publishing messages without a
     * partitioning key), it might be beneficial to increase this parameter to allow for more pipelining within the
     * individual partitions producers.
     */
    private int maxPendingMessagesAcrossPartitions;

    /**
     * Set the time period within which the messages sent will be batched <i>default: 1 ms</i> if batch messages are
     * enabled. If set to a non zero value, messages will be queued until either:
     * <ul>
     * <li>this time interval expires</li>
     * <li>the max number of messages in a batch is reached ({@link #batchingMaxMessages(int)})
     * <li>the max size of batch is reached
     * </ul>
     *
     * <p>
     * All messages will be published as a single batch message. The consumer will be delivered individual messages in
     * the batch in the same order they were enqueued.
     */
    private long batchingMaxPublishDelay;

    /**
     * Set the send timeout <i>(default: 30 seconds)</i>.
     *
     * <p>
     * If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
     *
     * <p>
     * Setting the timeout to zero, for example {@code setTimeout(0, TimeUnit.SECONDS)} will set the timeout to
     * infinity, which can be useful when using Pulsar's message deduplication feature, since the client library will
     * retry forever to publish a message. No errors will be propagated back to the application.
     */
    private int sendTimeout;

    /**
     * Control whether automatic batching of messages is enabled for the producer. <i>default: enabled</i>
     *
     * <p>
     * When batching is enabled, multiple calls to {@link Producer#sendAsync} can result in a single batch to be sent to
     * the broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
     * messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
     * contents.
     *
     * <p>
     * When enabled default batch delay is set to 1 ms and default batch size is 1000 messages
     *
     * <p>
     * Batching is enabled by default since 2.0.0.
     */
    private boolean enableBatching = true;

    /**
     * Set whether the {@link Producer#send} and {@link Producer#sendAsync} operations should block when the outgoing
     * message queue is full.
     *
     * <p>
     * Default is {@code false}. If set to {@code false}, send operations will immediately fail with
     * {@link ProducerQueueIsFullError} when there is no space left in pending queue. If set to {@code true}, the
     * {@link Producer#sendAsync} operation will instead block.
     *
     * <p>
     * Setting {@code blockIfQueueFull=true} simplifies the task of an application that just wants to publish messages
     * as fast as possible, without having to worry about overflowing the producer send queue.
     *
     * <p>
     * For example:
     * 
     * <pre>
     * <code>
     * Producer&lt;String&gt; producer = client.newProducer()
     *                  .topic("my-topic")
     *                  .blockIfQueueFull(true)
     *                  .create();
     *
     * while (true) {
     *     producer.sendAsync("my-message")
     *          .thenAccept(messageId -> {
     *              System.out.println("Published message: " + messageId);
     *          })
     *          .exceptionally(ex -> {
     *              System.err.println("Failed to publish: " + e);
     *              return null;
     *          });
     * }
     * </code>
     * </pre>
     */
    private boolean blockIfQueueFull;
}
