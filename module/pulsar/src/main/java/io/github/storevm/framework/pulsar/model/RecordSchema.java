package io.github.storevm.framework.pulsar.model;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Setter
@Getter
public class RecordSchema<K, V> {
    private Schema schema; // 消息的schema
    private Map<Field, RecordSchema> map = new HashMap();

    public RecordSchema() {}

    /**
     * constructor
     * 
     * @param key
     * @param value
     */
    public RecordSchema(final K key, final V value) {
        this.schema = toSchema(value);
    }

    /**
     * constructor
     * 
     * @param valueClass
     */
    public RecordSchema(final Class<V> valueClass) {
        this.schema = toSchema(valueClass);
        if (this.schema == null) {
            this.schema = toGenericSchema(valueClass, this);
        }
    }

    public void addChild(Field field, RecordSchema child) {
        map.put(field, child);
    }

    private Schema toSchema(final V value) {
        Schema schema = toSchema((Class<V>)value.getClass());
        if (schema == null) {
            schema = toGenericSchema((Class<V>)value.getClass(), this);
        }
        return schema;
    }

    /**
     * 解析基本类型的Schema
     * 
     * @param valueClass
     * @return
     */
    private Schema toSchema(Class<V> valueClass) {
        if (valueClass != null) {
            if (ClassUtils.isAssignable(valueClass, byte[].class)
                || ClassUtils.isAssignable(valueClass, ByteBuffer.class)
                || ClassUtils.isAssignable(valueClass, ByteBuf.class)) {
                return Schema.BYTES;
            } else if (ClassUtils.isAssignable(valueClass, String.class)
                || ClassUtils.isAssignable(valueClass, BigDecimal.class)
                || ClassUtils.isAssignable(valueClass, BigInteger.class)) {
                return Schema.STRING;
            } else if (ClassUtils.isAssignable(valueClass, Integer.class)) {
                return Schema.INT32;
            } else if (ClassUtils.isAssignable(valueClass, Long.class)) {
                return Schema.INT64;
            } else if (ClassUtils.isAssignable(valueClass, Short.class)) {
                return Schema.INT16;
            } else if (ClassUtils.isAssignable(valueClass, Byte.class)) {
                return Schema.INT8;
            } else if (ClassUtils.isAssignable(valueClass, Float.class)) {
                return Schema.FLOAT;
            } else if (ClassUtils.isAssignable(valueClass, Double.class)) {
                return Schema.DOUBLE;
            } else if (ClassUtils.isAssignable(valueClass, Boolean.class)) {
                return Schema.BOOL;
            } else if (ClassUtils.isAssignable(valueClass, Date.class)) {
                return Schema.DATE;
            } else if (ClassUtils.isAssignable(valueClass, Timestamp.class)) {
                return Schema.TIMESTAMP;
            } else if (ClassUtils.isAssignable(valueClass, Time.class)) {
                return Schema.TIME;
            }
        }
        return null;
    }

    /**
     * 解析泛型的Schema
     * 
     * @param value
     * @param metadata
     * @return
     */
    private GenericSchema<GenericRecord> toGenericSchema(Class<V> valueClass, RecordSchema metadata) {
        Field[] fields = FieldUtils.getAllFields(valueClass);
        RecordSchemaBuilder builder = SchemaBuilder.record(ClassUtils.getShortClassName(valueClass));
        for (int i = 0, n = fields.length; i < n; i++) {
            if (!StringUtils.equalsIgnoreCase("serialVersionUID", fields[i].getName())) {
                if (ClassUtils.isAssignable(fields[i].getType(), Boolean.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.BOOLEAN);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Byte.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT8);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Short.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT16);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Integer.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT32);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Long.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.INT64);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Float.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.FLOAT);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Double.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.DOUBLE);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), byte[].class)
                    || ClassUtils.isAssignable(fields[i].getType(), ByteBuffer.class)
                    || ClassUtils.isAssignable(fields[i].getType(), ByteBuf.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.BYTES);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), String.class)
                    || ClassUtils.isAssignable(fields[i].getType(), BigDecimal.class)
                    || ClassUtils.isAssignable(fields[i].getType(), BigInteger.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.STRING);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Timestamp.class)
                    || ClassUtils.isAssignable(fields[i].getType(), Date.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.TIMESTAMP);
                    metadata.addChild(fields[i], null);
                } else if (ClassUtils.isAssignable(fields[i].getType(), Time.class)) {
                    builder.field(fields[i].getName()).type(SchemaType.TIME);
                    metadata.addChild(fields[i], null);
                } else {
                    // 复杂对象
                    RecordSchema m = new RecordSchema();
                    GenericSchema<GenericRecord> s = toGenericSchema((Class<V>)fields[i].getType(), m);
                    builder.field(fields[i].getName(), s).type(SchemaType.AVRO);
                    m.setSchema(s);
                    metadata.addChild(fields[i], m);
                }
            }
        }
        SchemaInfo schemaInfo = builder.build(SchemaType.AVRO);
        return Schema.generic(schemaInfo);
    }
}
