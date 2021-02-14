package io.github.storevm.framework.pulsar.producer.impl;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import io.github.storevm.framework.pulsar.config.PulsarProducerConfig;
import io.github.storevm.framework.pulsar.exception.OnExceptionContext;
import io.github.storevm.framework.pulsar.model.Message;
import io.github.storevm.framework.pulsar.model.PulsarMessage;
import io.github.storevm.framework.pulsar.model.SendResult;
import io.github.storevm.framework.pulsar.producer.SendCallback;

/**
 * @author Jack
 * @date 2021/02/14
 */
public class AsyncPulsarProducer<K, V> extends PulsarProducer<K, V> {
    private SendCallback<K, V> callback; // 回调函数

    /**
     * constructor
     * 
     * @param client
     * @param config
     * @param key
     * @param value
     */
    public AsyncPulsarProducer(PulsarClient client, PulsarProducerConfig config, K key, V value) {
        super(client, config, key, value);
    }

    @Override
    public void setCallback(SendCallback<K, V> callback) {
        this.callback = callback;
    }

    /**
     * @see io.github.storevm.framework.pulsar.producer.impl.PulsarProducer#send(io.github.storevm.framework.pulsar.model.Message)
     */
    @Override
    public SendResult<K, V> send(final Message message) {
        if (message != null && (message instanceof PulsarMessage)) {
            // 转换消息类型
            PulsarMessage<K, V> pm = (PulsarMessage<K, V>)message;
            if (producer != null) {
                TypedMessageBuilder builder = toMessageBuilder(pm);
                // 发送异步消息
                CompletableFuture<MessageId> future = builder.sendAsync();
                future.thenAccept(id -> {
                    SendResult<K, V> result = toSendResult(pm, id);
                    this.callback.onSuccess(result);
                }).exceptionally(ex -> {
                    OnExceptionContext context = new OnExceptionContext();
                    context.setException(new RuntimeException(ex));
                    context.setTopic(message.getTopic());
                    this.callback.onException(context);
                    return null;
                });
            }
        }
        return null;
    }
}
