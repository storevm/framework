package io.github.storevm.framework.pulsar.model;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2021/12/31
 * @version 1.0.0
 */
@Setter
@Getter
public class GenericSchemaMetadata {
    private GenericSchema<GenericRecord> schema;
    private Map<Field, GenericSchemaMetadata> map = new HashMap();

    public GenericSchemaMetadata() {}

    public void addChild(Field field, GenericSchemaMetadata child) {
        map.put(field, child);
    }
}
