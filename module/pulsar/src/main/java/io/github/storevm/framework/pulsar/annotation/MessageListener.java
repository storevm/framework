package io.github.storevm.framework.pulsar.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.stereotype.Component;

/**
 * @author Jack
 * @date 2020/07/08
 * @version 1.0.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Component
public @interface MessageListener {
    /**
     * 订阅的主题集合
     * 
     * @return
     */
    String[] topics() default {};
}
