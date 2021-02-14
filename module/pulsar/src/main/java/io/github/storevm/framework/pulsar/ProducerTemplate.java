package io.github.storevm.framework.pulsar;

import java.util.Properties;

import io.github.storevm.framework.pulsar.model.Message;
import io.github.storevm.framework.pulsar.model.Message.SystemPropKey;
import io.github.storevm.framework.pulsar.model.ProducerTypeEnum;
import io.github.storevm.framework.pulsar.model.SendResult;
import io.github.storevm.framework.pulsar.producer.MessagingAccessPoint;
import io.github.storevm.framework.pulsar.producer.Producer;
import io.github.storevm.framework.pulsar.producer.SendCallback;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Jack
 * @date 2021/12/30
 * @version 1.0.0
 */
@Slf4j
public class ProducerTemplate<K, V> {
    static final long DEFAULT_TIMEOUT = 5000; // 默认超时
    private MessagingAccessPoint<K, V> point; // 接入点

    /**
     * constructor
     * 
     * @param point
     */
    public ProducerTemplate(MessagingAccessPoint<K, V> point) {
        this.point = point;
    }

    /**
     * 发送同步消息
     * 
     * @param topic
     *            主题
     * @param key
     *            消息键值
     * @param body
     *            消息体
     * @param timeout
     *            超时时间(ms)
     * @return
     */
    public SendResult<K, V> syncSend(String topic, final K key, final V value, long timeout) {
        if (this.point != null) {
            Properties props = new Properties();
            props.put(SystemPropKey.PRODUCERTYPE, ProducerTypeEnum.SYNC); // 同步生产者
            try {
                Producer<K, V> producer = this.point.createProducer(topic, key, value, props);
                if (producer != null) {
                    // 编码消息
                    Message message = producer.messageBuilder().withTopic(topic).withKey(key).withValue(value).build();
                    message.getUserProperties().put(SystemPropKey.TIMEOUT, String.valueOf(timeout));// 设置属性值
                    // 发送消息
                    return (SendResult<K, V>)producer.send(message);
                }
            } catch (Exception ex) {
                log.error("发送同步消息时发生异常", ex);
            }
        }
        return null;
    }

    /**
     * 同步发送消息
     * 
     * @param topic
     * @param message
     * @return
     */
    public SendResult<K, V> syncSend(String topic, final K key, final V body) {
        return syncSend(topic, key, body, DEFAULT_TIMEOUT);
    }

    /**
     * 发送同步延迟消息
     * 
     * @param topic
     *            主题
     * @param key
     *            消息键值
     * @param value
     *            消息体
     * @param delay
     *            延迟时间（单位：毫秒）
     * @return
     */
    public SendResult<K, V> delayedDelivery(String topic, final K key, final V value, long delay) {
        if (this.point != null) {
            Properties props = new Properties();
            props.put(SystemPropKey.PRODUCERTYPE, ProducerTypeEnum.DELAYED); // 延迟发送生产者
            try {
                Producer<K, V> producer = this.point.createProducer(topic, key, value, props);
                if (producer != null) {
                    producer.setDelay(delay); // 设置延迟消息
                    // 编码消息
                    Message message = producer.messageBuilder().withTopic(topic).withKey(key).withValue(value).build();
                    // 发送消息
                    return (SendResult<K, V>)producer.send(message);
                }
            } catch (Exception ex) {
                log.error("发送同步延迟消息时发生异常", ex);
            }
        }
        return null;
    }

    /**
     * 异步发送消息
     * 
     * @param topic
     * @param key
     * @param body
     * @param callback
     */
    public void asyncSend(String topic, final K key, final V value, final SendCallback<K, V> callback) {
        if (this.point != null) {
            Properties props = new Properties();
            props.put(SystemPropKey.PRODUCERTYPE, ProducerTypeEnum.ASYNC); // 异步生产者
            try {
                Producer<K, V> producer = this.point.createProducer(topic, key, value, props);
                if (producer != null) {
                    producer.setCallback(callback); // 设置回调函数
                    // 编码消息
                    Message message = producer.messageBuilder().withTopic(topic).withKey(key).withValue(value).build();
                    message.getUserProperties().put(SystemPropKey.TIMEOUT, DEFAULT_TIMEOUT);// 设置属性值
                    producer.send(message);
                }
            } catch (Exception ex) {
                log.error("发送异步消息时发生异常", ex);
            }
        }
    }
}
