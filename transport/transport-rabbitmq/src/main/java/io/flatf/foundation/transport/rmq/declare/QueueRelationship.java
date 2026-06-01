package io.flatf.foundation.transport.rmq.declare;

import io.flatf.foundation.common.collections.MutableLists;
import io.flatf.foundation.common.lang.Validator;
import io.flatf.foundation.serialization.json.JsonWriter;
import io.flatf.foundation.transport.rmq.RmqOperator;
import io.flatf.foundation.transport.rmq.exception.DeclareException;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.Map;

/**
 * 定义Queue和其他实体绑定关系
 *
 * @author yellow013
 */
@Getter
public final class QueueRelationship extends Relationship {

    /**
     * queue
     */
    private final AmqpQueue queue;

    /**
     * @param name String
     * @return QueueRelationship
     */
    public static QueueRelationship named(String name) {
        return withQueue(AmqpQueue.named(name));
    }

    /**
     * @param queue AmqpQueue
     * @return QueueRelationship
     */
    public static QueueRelationship withQueue(AmqpQueue queue) {
        Validator.nonNull(queue, "queue");
        return new QueueRelationship(queue);
    }

    private QueueRelationship(AmqpQueue queue) {
        this.queue = queue;
    }

    @Override
    protected void declare0(RmqOperator operator) {
        try {
            operator.declareQueue(queue);
        } catch (DeclareException e) {
            log.error("Declare Queue failure -> {}", queue);
            throw new RuntimeException(e);
        }
    }

    /**
     * @return the queue name
     */
    public String queueName() {
        return queue.name();
    }

    /**
     * 是否持久化, 默认true
     *
     * @param durable boolean
     * @return QueueRelationship
     */
    public QueueRelationship durable(boolean durable) {
        queue.durable(durable);
        return this;
    }

    /**
     * channel关闭后自动删除队列, 默认false
     *
     * @param autoDelete boolean
     * @return QueueRelationship
     */
    public QueueRelationship autoDelete(boolean autoDelete) {
        queue.autoDelete(autoDelete);
        return this;
    }

    /**
     * 连接独占此队列, 默认false
     *
     * @param exclusive boolean
     * @return QueueRelationship
     */
    public QueueRelationship exclusive(boolean exclusive) {
        queue.exclusive(exclusive);
        return this;
    }

    /**
     * @param args Map<String, Object>
     * @return QueueRelationship
     */
    public QueueRelationship args(Map<String, Object> args) {
        queue.args(args);
        return this;
    }

    /**
     * @param exchanges AmqpExchange[]
     * @return QueueRelationship
     */
    public QueueRelationship binding(AmqpExchange... exchanges) {
        return binding(exchanges != null
                ? MutableLists.newFastList(exchanges) : null, null);
    }

    /**
     * @param exchanges   Collection<AmqpExchange>
     * @param routingKeys Collection<String>
     * @return QueueRelationship
     */
    public QueueRelationship binding(Collection<AmqpExchange> exchanges, Collection<String> routingKeys) {
        if (exchanges != null) {
            exchanges.forEach(exchange -> {
                if (CollectionUtils.isNotEmpty(routingKeys))
                    routingKeys.forEach(routingKey ->
                            bindings.add(new Binding(exchange, queue, routingKey)));
                else
                    bindings.add(new Binding(exchange, queue));
            });
        }
        return this;
    }

    @Override
    public String toString() {
        return JsonWriter.toJsonHasNulls(this);
    }

    public static void main(String[] args) {
        System.out.println(
                QueueRelationship
                        .named("TEST")
                        .binding(AmqpExchange
                                .fanout("T0"))
                        .binding(AmqpExchange
                                .fanout("T1")));
    }

}
