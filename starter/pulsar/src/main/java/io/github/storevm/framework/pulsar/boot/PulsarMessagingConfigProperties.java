package io.github.storevm.framework.pulsar.boot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.PulsarClient;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2020/07/02
 * @version 1.0.0
 */
@Setter
@Getter
public class PulsarMessagingConfigProperties implements Serializable {
    /**
     * UID
     */
    private static final long serialVersionUID = -2380257935772784L;

    /**
     * Configure the service URL for the Pulsar service.
     */
    private String serviceUrl;

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     */
    private String authentication;

    /**
     * Set the operation timeout <i>(default: 30 seconds)</i>.
     */
    private int operationTimeout;

    /**
     * Number of max lookup-requests allowed on each broker-connection to prevent overload on broker. <i>(default:
     * 50000)</i> It should be bigger than maxConcurrentLookupRequests. Requests that inside maxConcurrentLookupRequests
     * already send to broker, and requests beyond maxConcurrentLookupRequests and under maxLookupRequests will wait in
     * each client cnx.
     *
     */
    private int maxLookupRequests;

    /**
     * Number of concurrent lookup-requests allowed to send on each broker-connection to prevent overload on broker.
     * <i>(default: 5000)</i> It should be configured with higher value only in case of it requires to produce/subscribe
     * on thousands of topic using created {@link PulsarClient}.
     *
     */
    private int maxConcurrentLookupRequests;

    /**
     * Set the duration of time to wait for a connection to a broker to be established. If the duration passes without a
     * response from the broker, the connection attempt is dropped.
     */
    private int connectionTimeout;

    /**
     * Set the duration of time for a backoff interval.
     */
    private int startingBackoffInterval;

    /**
     * Set keep alive interval for each client-broker-connection. <i>(default: 30 seconds)</i>.
     */
    private int keepAliveInterval;

    /**
     * Set max number of broker-rejected requests in a certain time-frame (30 seconds) after which current connection
     * will be closed and client creates a new connection that give chance to connect a different broker <i>(default:
     * 50)</i>.
     */
    private int maxNumberOfRejectedRequestPerConnection;

    /**
     * Set the number of threads to be used for handling connections to brokers <i>(default: 1 thread)</i>.
     */
    private int ioThreads;

    /**
     * Set the number of threads to be used for message listeners <i>(default: 1 thread)</i>.
     *
     * <p>
     * The listener thread pool is shared across all the consumers and readers that are using a "listener" model to get
     * messages. For a given consumer, the listener will be always invoked from the same thread, to ensure ordering.
     */
    private int listenerThreads;

    /**
     * Set the maximum duration of time for a backoff interval.
     */
    private long maxBackoffInterval;

    /**
     * Set the interval between each stat info <i>(default: 60 seconds)</i> Stats will be activated with positive
     * statsInterval It should be set to at least 1 second.
     */
    private int statsInterval;

    /**
     * Configure whether the Pulsar client accept untrusted TLS certificate from broker <i>(default: false)</i>.
     */
    private boolean allowTlsInsecureConnection;

    /**
     * It allows to validate hostname verification when client connects to broker over tls. It validates incoming x509
     * certificate and matches provided hostname(CN/SAN) with expected broker's host name. It follows RFC 2818, 3.1.
     * Server Identity hostname verification.
     *
     * @see <a href="https://tools.ietf.org/html/rfc2818">RFC 818</a>
     */
    private boolean enableTlsHostnameVerification;

    /**
     * Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
     *
     * <p>
     * No-delay features make sure packets are sent out on the network as soon as possible, and it's critical to achieve
     * low latency publishes. On the other hand, sending out a huge number of small packets might limit the overall
     * throughput, so if latency is not a concern, it's advisable to set the <code>useTcpNoDelay</code> flag to false.
     *
     * <p>
     * Default value is true.
     */
    private boolean enableTcpNoDelay;

    /**
     * 生产者配置
     */
    private Map<String, PulsarProducerConfigProperties> producers;

    /**
     * 消费者配置
     */
    private Map<String, PulsarConsumerConfigProperties> consumers;

    /**
     * constructor
     */
    public PulsarMessagingConfigProperties() {
        this.producers = new HashMap();
        this.consumers = new HashMap();
    }
}
