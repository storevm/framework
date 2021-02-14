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
import io.github.storevm.framework.pulsar.model.RecordSchema;

/**
 * @author Jack
 * @date 2021/12/31
 * @version 1.0.0
 */
public class GenericRecordCreator<K, V> {
    private RecordSchema<K, V> record;
    private FieldConversion conversion;

    protected GenericRecordCreator(RecordSchema record) {
        this.record = record;
        this.conversion = new FieldConversion();
    }

    public static GenericRecordCreator newInstance(RecordSchema record) {
        return new GenericRecordCreator(record);
    }

    public GenericRecord create(V value) throws IllegalAccessException {
        return toGenericRecord(value, this.record, (GenericSchema<GenericRecord>)this.record.getSchema());
    }

    private GenericRecord toGenericRecord(V value, RecordSchema record, GenericSchema<GenericRecord> schema)
        throws IllegalAccessException {
        GenericRecordBuilder builder = schema.newRecordBuilder();
        Set<Entry<Field, RecordSchema>> set = record.getMap().entrySet();
        Iterator<Entry<Field, RecordSchema>> it = set.iterator();
        while (it.hasNext()) {
            Entry<Field, RecordSchema> entry = it.next();
            V val = (V)FieldUtils.readField(entry.getKey(), value, true);
            if (entry.getValue() == null) { // 简单对象
                builder.set(entry.getKey().getName(), this.conversion.convert(val));
            } else { // 复杂对象
                builder.set(entry.getKey().getName(),
                    toGenericRecord(val, entry.getValue(), (GenericSchema<GenericRecord>)entry.getValue().getSchema()));
            }
        }
        GenericRecord gr = builder.build();
        return gr;
    }
}
