package io.github.storevm.framework.pulsar.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.commons.lang3.ClassUtils;

/**
 * @author Jack
 * @date 2021/02/07
 * @version 1.0.0
 */
public class FieldConversion {
    public Object convert(Object value) {
        if (value instanceof Date) {
            return ((Date)value).getTime();
        } else if (value instanceof Timestamp) {
            return ((Timestamp)value).getTime();
        } else if (value instanceof Time) {
            return ((Time)value).getTime();
        } else if (value instanceof BigDecimal) {
            return ((BigDecimal)value).toString();
        } else if (value instanceof BigInteger) {
            return ((BigInteger)value).toString();
        }
        return value;
    }

    public Object from(Class cls, Object value) {
        if (ClassUtils.isAssignable(cls, Date.class)) {
            return new Date(Long.valueOf(String.valueOf(value)));
        } else if (ClassUtils.isAssignable(cls, Timestamp.class)) {
            return new Timestamp(Long.valueOf(String.valueOf(value)));
        } else if (ClassUtils.isAssignable(cls, Time.class)) {
            return new Time(Long.valueOf(String.valueOf(value)));
        } else if (ClassUtils.isAssignable(cls, BigDecimal.class)) {
            return new BigDecimal(String.valueOf(value));
        } else if (ClassUtils.isAssignable(cls, BigInteger.class)) {
            return new BigInteger(String.valueOf(value));
        }
        return value;
    }
}
