package io.github.storevm.framework.pulsar.annotation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import io.github.storevm.framework.pulsar.consumer.ConsumerRegistry;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Jack
 * @date 2020/07/08
 * @version 1.0.0
 */
@Slf4j
@Component
public class MessageListenerAnnotationBeanPostProcessor
    implements BeanPostProcessor, BeanFactoryAware, ApplicationContextAware {
    private BeanFactory beanFactory;

    private BeanExpressionContext expressionContext;

    private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

    private ApplicationContext applicationContext;

    /**
     * @see org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessorAdapter#postProcessAfterInitialization(java.lang.Object,
     *      java.lang.String)
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        io.github.storevm.framework.pulsar.annotation.MessageListener annotation = AnnotationUtils
            .findAnnotation(bean.getClass(), io.github.storevm.framework.pulsar.annotation.MessageListener.class);
        if ((bean instanceof MessageListener) && annotation != null) {
            String[] topics = resolveTopic(annotation);
            log.info("解析消息监听器的主题, topics={}", Arrays.toString(topics));
            // 从spring上下文中找到消费者注册器
            Map<String, ConsumerRegistry> registries = applicationContext.getBeansOfType(ConsumerRegistry.class);
            Set<Entry<String, ConsumerRegistry>> set = registries.entrySet();
            Iterator<Entry<String, ConsumerRegistry>> it = set.iterator();
            while (it.hasNext()) {
                it.next().getValue().addListener(topics,
                    (io.github.storevm.framework.pulsar.consumer.MessageListener)bean);
            }
        }
        return bean;
    }

    /**
     * @see org.springframework.beans.factory.BeanFactoryAware#setBeanFactory(org.springframework.beans.factory.BeanFactory)
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
        if (beanFactory instanceof ConfigurableListableBeanFactory) {
            this.resolver = ((ConfigurableListableBeanFactory)beanFactory).getBeanExpressionResolver();
            this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory)beanFactory, null);
        }
    }

    /**
     * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private String[] resolveTopic(io.github.storevm.framework.pulsar.annotation.MessageListener annotation) {
        String[] topics = annotation.topics();
        String[] values = new String[topics.length];
        if (topics != null && topics.length > 0) {
            for (int i = 0, n = topics.length; i < n; i++) {
                values[i] = (String)resolveExpression(topics[i]);
            }
        }
        return values;
    }

    private Object resolveExpression(String value) {
        return this.resolver.evaluate(resolve(value), this.expressionContext);
    }

    private String resolve(String value) {
        if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
            return ((ConfigurableBeanFactory)this.beanFactory).resolveEmbeddedValue(value);
        }
        return value;
    }
}
