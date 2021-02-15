package io.github.storevm.framework.pulsar.model;

import java.io.Serializable;
import java.util.Properties;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Setter
@Getter
public class Message implements Serializable {
    /**
     * UID
     */
    private static final long serialVersionUID = 2887873450781046882L;

    private String topic;
    private Properties userProperties;

    /**
     * constructor
     * 
     * @param topic
     * @param key
     * @param value
     */
    public Message(String topic) {
        this.topic = topic;
    }

    public void putUserProperties(final String key, final String value) {
        if (null == this.userProperties) {
            this.userProperties = new Properties();
        }

        if (key != null && value != null) {
            this.userProperties.put(key, value);
        }
    }

    public String getUserProperties(final String key) {
        if (null != this.userProperties) {
            return (String)this.userProperties.get(key);
        }

        return null;
    }

    public Class getKeyClass() {
        String classname = this.getUserProperties(SystemPropKey.KEYCLASS);
        if (classname != null) {
            try {
                return Class.forName(classname);
            } catch (ClassNotFoundException ex) {
            }
        }
        return byte[].class;
    }

    public Class getValueClass() {
        String classname = this.getUserProperties(SystemPropKey.VALUECLASS);
        if (classname != null) {
            try {
                return Class.forName(classname);
            } catch (ClassNotFoundException ex) {
            }
        }
        return byte[].class;
    }

    public String getTag() {
        return this.getUserProperties(SystemPropKey.TAG);
    }

    public void setTag(String tag) {
        this.putUserProperties(SystemPropKey.TAG, tag);
    }

    public String getMsgID() {
        return this.getUserProperties(SystemPropKey.MSGID);
    }

    public void setMsgID(String msgid) {
        this.putUserProperties(SystemPropKey.MSGID, msgid);
    }

    public Properties getUserProperties() {
        if (null == userProperties) {
            this.userProperties = new Properties();
        }
        return userProperties;
    }

    public int getReconsumeTimes() {
        String pro = this.getUserProperties(SystemPropKey.RECONSUMETIMES);
        if (pro != null) {
            return Integer.parseInt(pro);
        }

        return 0;
    }

    public void setReconsumeTimes(final int value) {
        putUserProperties(SystemPropKey.RECONSUMETIMES, String.valueOf(value));
    }

    public long getBornTimestamp() {
        String pro = this.getUserProperties(SystemPropKey.BORNTIMESTAMP);
        if (pro != null) {
            return Long.parseLong(pro);
        }

        return 0L;
    }

    public void setBornTimestamp(final long value) {
        putUserProperties(SystemPropKey.BORNTIMESTAMP, String.valueOf(value));
    }

    public String getBornHost() {
        String pro = this.getUserProperties(SystemPropKey.BORNHOST);
        return pro == null ? "" : pro;
    }

    public void setBornHost(final String value) {
        putUserProperties(SystemPropKey.BORNHOST, value);
    }

    public long getStartDeliverTime() {
        String pro = this.getUserProperties(SystemPropKey.STARTDELIVERTIME);
        if (pro != null) {
            return Long.parseLong(pro);
        }

        return 0L;
    }

    public String getShardingKey() {
        String pro = this.getUserProperties(SystemPropKey.SHARDINGKEY);
        return pro == null ? "" : pro;
    }

    public void setShardingKey(final String value) {
        putUserProperties(SystemPropKey.SHARDINGKEY, value);
    }

    public void setStartDeliverTime(final long value) {
        putUserProperties(SystemPropKey.STARTDELIVERTIME, String.valueOf(value));
    }

    /**
     * Get the offset of this message assigned by the broker.
     *
     * @return Message offset in relative partition
     */
    public long getOffset() {
        String v = getUserProperties(SystemPropKey.CONSUMEOFFSET);
        if (v != null) {
            return Long.parseLong(v);
        }
        return 0;
    }

    /**
     * Get the partition to which the message belongs.
     *
     * @return Message offset in relative partition
     */
    public TopicPartition getTopicPartition() {
        String v = getUserProperties(SystemPropKey.PARTITION);
        Integer partition = v != null ? Integer.valueOf(v) : -1;
        return new TopicPartition(topic, partition);
    }

    public Long getTimeout(Long def) {
        Long timeout = Long.valueOf(this.getUserProperties(SystemPropKey.TIMEOUT));
        return timeout != null ? timeout : def;
    }

    static public class SystemPropKey {
        public static final String TAG = "__TAG";
        public static final String KEY = "__KEY";
        public static final String MSGID = "__MSGID";
        public static final String SHARDINGKEY = "__SHARDINGKEY";
        public static final String RECONSUMETIMES = "__RECONSUMETIMES";
        public static final String BORNTIMESTAMP = "__BORNTIMESTAMP";
        public static final String BORNHOST = "__BORNHOST";

        public static final String STARTDELIVERTIME = "__STARTDELIVERTIME";

        public static final String CONSUMEOFFSET = "__CONSUMEOFFSET";

        public static final String PARTITION = "__PARTITION";

        public static final String KEYCLASS = "__KEYCLASS";
        public static final String VALUECLASS = "__VALUECLASS";
        public static final String TIMEOUT = "__TIMEOUT";
        public static final String PRODUCERTYPE = "__PRODUCERTYPE";
    }
}
