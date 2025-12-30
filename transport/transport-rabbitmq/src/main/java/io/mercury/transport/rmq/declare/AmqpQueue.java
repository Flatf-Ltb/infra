package io.mercury.transport.rmq.declare;

import io.mercury.common.collections.MapUtil;
import io.mercury.common.lang.Validator;
import io.mercury.serialization.json.JsonWriter;

import javax.annotation.Nonnull;
import java.util.Map;

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

    public AmqpQueue setDurable(boolean durable) {
        this.durable = durable;
        return this;
    }

    public AmqpQueue setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
        return this;
    }

    public AmqpQueue setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
        return this;
    }

    public AmqpQueue setArgs(Map<String, Object> args) {
        this.args = args;
        return this;
    }

    public String getName() {
        return name;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public Map<String, Object> getArgs() {
        return args;
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
