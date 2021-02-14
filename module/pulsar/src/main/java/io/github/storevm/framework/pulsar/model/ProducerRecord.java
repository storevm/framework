package io.github.storevm.framework.pulsar.model;

/**
 * @author Jack
 * @date 2020/07/14
 * @version 1.0.0
 */
public final class ProducerRecord<K, V> {
    private final String topic;
    private final Integer partition;
    private final K key;
    private final V value;

    /**
     * Creates a record to be sent to a specified topic and partition
     * 
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
    }

    /**
     * Create a record to be sent to Kafka
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, key, value);
    }

    /**
     * Create a record with no key
     * 
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, value);
    }

    /**
     * The topic this record is being sent to
     */
    public String topic() {
        return topic;
    }

    /**
     * The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }

    /**
     * @return The value
     */
    public V value() {
        return value;
    }

    /**
     * The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        return partition;
    }

    @Override
    public String toString() {
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", key=" + key + ", value=" + value;
    }
}
