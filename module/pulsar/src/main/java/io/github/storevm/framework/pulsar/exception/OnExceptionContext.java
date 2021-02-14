package io.github.storevm.framework.pulsar.exception;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Jack
 * @date 2021/02/14
 */
@Setter
@Getter
public class OnExceptionContext {
    private String messageId;
    private String topic;
    /**
     * Detailed exception stack information.
     */
    private RuntimeException exception;
}
