package io.flatf.foundation.transport.rmq.declare;

import io.flatf.foundation.common.collections.MapUtil;
import io.flatf.foundation.common.lang.Validator;
import io.flatf.foundation.serialization.json.JsonWriter;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.Map;

@Getter
@Setter
@Accessors(chain = true, fluent = true)
public final class AmqpQueue {

    // 队列名称
    private final String name;

    // 是否持久化
    private boolean durable = true;

    // 连接独占此队列
    private boolean exclusive = false;

    // channel关闭后自动删除队列
    private boolean autoDelete = false;

    // 队列参数
    private Map<String, Object> args = null;

    private AmqpQueue(String name) {
        this.name = name;
    }

    /**
     * 定义队列
     *
     * @param name String
     * @return AmqpQueue
     */
    public static AmqpQueue named(@Nonnull String name) {
        Validator.nonEmpty(name, "name");
        return new AmqpQueue(name);
    }

    @Override
    public String toString() {
        return JsonWriter.toJsonHasNulls(this);
    }

    public boolean isIdempotent(AmqpQueue another) {
        if (another == null)
            return false;
        return name.equals(another.name)
               && durable == another.durable
               && exclusive == another.exclusive
               && autoDelete == another.autoDelete
               && MapUtil.isEquals(args, another.args);
    }

}
