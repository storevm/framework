package io.github.storevm.framework.pulsar.model;

/**
 * @author Jack
 * @date 2020/07/14
 * @version 1.0.0
 */
public class SendResult<K, V> {
    private final ProducerRecord<K, V> producerRecord;
    private final RecordMetadata recordMetadata;

    public SendResult(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
        this.producerRecord = producerRecord;
        this.recordMetadata = recordMetadata;
    }

    public ProducerRecord<K, V> getProducerRecord() {
        return this.producerRecord;
    }

    public RecordMetadata getRecordMetadata() {
        return this.recordMetadata;
    }

    /**
     * The topic this record is being sent to
     */
    public String topic() {
        if (producerRecord != null) {
            return producerRecord.topic();
        }
        return null;
    }

    /**
     * The key (or null if no key is specified)
     */
    public K key() {
        if (producerRecord != null) {
            return producerRecord.key();
        }
        return null;
    }

    /**
     * @return The value
     */
    public V value() {
        if (producerRecord != null) {
            return producerRecord.value();
        }
        return null;
    }

    /**
     * The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        if (producerRecord != null) {
            return producerRecord.partition();
        }
        return -1;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        if (recordMetadata != null) {
            return recordMetadata.timestamp();
        }
        return -1L;
    }

    @Override
    public String toString() {
        return "SendResult [producerRecord=" + this.producerRecord + ", recordMetadata=" + this.recordMetadata + "]";
    }
}
