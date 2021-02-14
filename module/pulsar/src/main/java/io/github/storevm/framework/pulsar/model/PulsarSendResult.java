package io.github.storevm.framework.pulsar.model;

import org.apache.pulsar.client.api.MessageId;

/**
 * @author Jack
 * @date 2021/02/14
 */
public class PulsarSendResult<K, V> extends SendResult<K, V> {
    private MessageId id;

    /**
     * constructor
     * 
     * @param producerRecord
     * @param recordMetadata
     */
    public PulsarSendResult(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
        super(producerRecord, recordMetadata);
    }

    /**
     * constructor
     * 
     * @param producerRecord
     * @param recordMetadata
     * @param id
     */
    public PulsarSendResult(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata, MessageId id) {
        this(producerRecord, recordMetadata);
        this.id = id;
    }

    public MessageId id() {
        return this.id;
    }
}
