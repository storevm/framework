package io.github.storevm.framework.pulsar.producer.impl;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;

import io.github.storevm.framework.pulsar.model.FieldConversion;
import io.github.storevm.framework.pulsar.model.GenericSchemaMetadata;

/**
 * @author Jack
 * @date 2021/12/31
 * @version 1.0.0
 */
public class GenericRecordCreator {
    private GenericSchemaMetadata metadata;
    private FieldConversion conversion;

    protected GenericRecordCreator(GenericSchemaMetadata metadata) {
        this.metadata = metadata;
        this.conversion = new FieldConversion();
    }

    public static GenericRecordCreator builder(GenericSchemaMetadata metadata) {
        return new GenericRecordCreator(metadata);
    }

    public <V> GenericRecord create(V value, GenericSchema<GenericRecord> schema) throws IllegalAccessException {
        return toGenericRecord(value, this.metadata, schema);
    }

    private <V> GenericRecord toGenericRecord(V value, GenericSchemaMetadata metadata,
        GenericSchema<GenericRecord> schema) throws IllegalAccessException {
        GenericRecordBuilder builder = schema.newRecordBuilder();
        Set<Entry<Field, GenericSchemaMetadata>> set = metadata.getMap().entrySet();
        Iterator<Entry<Field, GenericSchemaMetadata>> it = set.iterator();
        while (it.hasNext()) {
            Entry<Field, GenericSchemaMetadata> entry = it.next();
            V val = (V)FieldUtils.readField(entry.getKey(), value, true);
            if (entry.getValue() == null) { // 简单对象
                builder.set(entry.getKey().getName(), this.conversion.convert(val));
            } else { // 复杂对象
                builder.set(entry.getKey().getName(),
                    toGenericRecord(val, entry.getValue(), entry.getValue().getSchema()));
            }
        }
        GenericRecord record = builder.build();
        return record;
    }
}
