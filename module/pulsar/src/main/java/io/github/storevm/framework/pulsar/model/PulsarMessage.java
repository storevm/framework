package io.github.storevm.framework.pulsar.model;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Setter
@Getter
public class PulsarMessage<K, V> extends Message {
    /**
     * UID
     */
    private static final long serialVersionUID = -4948989100926238093L;

    private K key;
    private V value;
    private FieldConversion conversion = new FieldConversion();

    /**
     * constructor
     * 
     * @param topic
     * @param key
     * @param value
     */
    public PulsarMessage(String topic, K key, V value) {
        super(topic);
        this.key = key;
        this.value = value;
    }

    /**
     * constructor
     * 
     * @param message
     */
    public PulsarMessage(org.apache.pulsar.client.api.Message message) {
        super(message.getTopicName());
        this.getUserProperties().putAll(message.getProperties());
        this.setMsgID(message.getMessageId().toString());
        this.setBornTimestamp(message.getPublishTime());
        this.setKey(toKey(getKeyClass(), message.getKey()));
        this.setBornHost(message.getProducerName());
        this.setValue((V)message.getValue());
    }

    public V getValue() {
        if (value instanceof GenericAvroRecord) {
            Class<V> cls = getValueClass();
            return toValue(cls, (GenericAvroRecord)value);
        }
        return value;
    }

    /**
     * 转换值的类型
     * 
     * @param cls
     * @param record
     * @return
     */
    private V toValue(Class<V> cls, GenericAvroRecord record) {
        try {
            // 实例化一个对象
            V object = ConstructorUtils.invokeConstructor(cls);
            Field[] fields = FieldUtils.getAllFields(cls);
            for (Field field : fields) {
                if (!StringUtils.equalsIgnoreCase("serialVersionUID", field.getName())) {
                    Object val = record.getField(field.getName());
                    if (val instanceof GenericAvroRecord) {
                        val = toValue((Class<V>)field.getType(), (GenericAvroRecord)val);
                    }
                    FieldUtils.writeField(field, object, conversion.from(field.getType(), val), true);
                }
            }
            return object;
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException
            | NoSuchMethodException ex) {
        }
        return value;
    }

    /**
     * 转换键值的类型
     * 
     * @param cls
     * @param key
     * @return
     */
    private K toKey(Class cls, String key) {
        if (cls != null && StringUtils.isNotBlank(key)) {
            if (ClassUtils.isAssignable(cls, String.class)) {
                return (K)key;
            } else if (ClassUtils.isAssignable(cls, Short.class)) {
                return (K)Short.valueOf(key);
            } else if (ClassUtils.isAssignable(cls, Integer.class)) {
                return (K)Integer.valueOf(key);
            } else if (ClassUtils.isAssignable(cls, Long.class)) {
                return (K)Long.valueOf(key);
            } else if (ClassUtils.isAssignable(cls, Float.class)) {
                return (K)Float.valueOf(key);
            } else if (ClassUtils.isAssignable(cls, Double.class)) {
                return (K)Double.valueOf(key);
            } else if (ClassUtils.isAssignable(cls, byte[].class)) {
                return (K)key.getBytes();
            }
        }
        return (K)null;
    }

}
