package io.flatf.transport.rmq.config;

import io.flatf.serialization.json.JsonWriter;
import io.flatf.transport.rmq.declare.ExchangeRelationship;
import io.flatf.transport.rmq.declare.QueueRelationship;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.Map;

import static io.flatf.common.lang.Validator.nonNull;

/**
 * @author yellow013
 */
@Getter
@Accessors(fluent = true)
public final class RmqReceiverConfig extends RmqConfig {

    // 接受者QueueDeclare
    private final QueueRelationship queue;

    // 错误消息ExchangeDeclare
    private final ExchangeRelationship errMsgExchange;

    // 错误消息RoutingKey
    private final String errMsgRoutingKey;

    // 错误消息QueueDeclare
    private final QueueRelationship errMsgQueue;

    // 消费者独占队列
    private final boolean exclusive;

    // ACK选项
    private final ReceiveAckOptions ackOptions;

    // 消费者参数, 默认为null
    private final Map<String, Object> args;

    /**
     * @param builder Builder
     */
    private RmqReceiverConfig(Builder builder) {
        super(builder.connection);
        this.queue = builder.queue;
        this.errMsgExchange = builder.errMsgExchange;
        this.errMsgRoutingKey = builder.errMsgRoutingKey;
        this.errMsgQueue = builder.errMsgQueue;
        this.exclusive = builder.exclusive;
        this.ackOptions = builder.ackOptions;
        this.args = builder.args;
    }

    /**
     * @param connection    RmqConnection
     * @param queue         QueueRelationship
     * @return Builder
     */
    public static Builder with(@Nonnull RmqConnectionConfig connection, @Nonnull QueueRelationship queue) {
        nonNull(connection, "connection");
        nonNull(queue, "queue");
        return new Builder(connection, queue);
    }

    private transient String toStringCache;

    @Override
    public String toString() {
        if (toStringCache == null)
            toStringCache = JsonWriter.toJson(this);
        return toStringCache;
    }

    @Setter
    @Accessors(chain = true, fluent = true)
    public static class Builder {

        // 连接配置
        private final RmqConnectionConfig connection;
        // 接受者QueueRelationship
        private final QueueRelationship queue;
        // 错误消息ExchangeRelationship

        // 错误消息处理Exchange和关联关系
        private ExchangeRelationship errMsgExchange;
        // 错误消息处理RoutingKey
        private String errMsgRoutingKey = "";
        // 错误消息处理QueueRelationship和关联关系
        private QueueRelationship errMsgQueue;

        // 接收者是否独占队列
        private boolean exclusive = false;

        // ACK选项
        private ReceiveAckOptions ackOptions = ReceiveAckOptions.withDefault();

        // 消费者参数, 默认为null
        private Map<String, Object> args = null;

        private Builder(RmqConnectionConfig connection, QueueRelationship queue) {
            this.connection = connection;
            this.queue = queue;
        }

        /**
         * @param autoAck boolean
         */
        public Builder autoAck(boolean autoAck) {
            this.ackOptions.autoAck(autoAck);
            return this;
        }

        /**
         * @param multipleAck boolean
         */
        public Builder multipleAck(boolean multipleAck) {
            this.ackOptions.multipleAck(multipleAck);
            return this;
        }

        /**
         * @param maxAckTotal int
         * @return Builder
         */
        public Builder maxAckTotal(int maxAckTotal) {
            this.ackOptions.maxAckTotal(maxAckTotal);
            return this;
        }

        /**
         * @param maxAckReconnection int
         * @return Builder
         */
        public Builder maxAckReconnection(int maxAckReconnection) {
            this.ackOptions.maxAckReconnection(maxAckReconnection);
            return this;
        }

        /**
         * @param qos int
         * @return Builder
         */
        public Builder qos(int qos) {
            this.ackOptions.qos(qos);
            return this;
        }

        public RmqReceiverConfig build() {
            return new RmqReceiverConfig(this);
        }

    }

    /**
     * @author yellow013
     */
    @Getter
    @Setter
    @Accessors(chain = true, fluent = true)
    public static final class ReceiveAckOptions {

        private boolean autoAck = true;         // 自动ACK, 默认true
        private boolean multipleAck = false;    // 一次ACK多条, 默认false
        private int maxAckTotal = 16;           // ACK最大自动重试次数, 默认16次
        private int maxAckReconnection = 8;     // ACK最大自动重连次数, 默认8次
        private int qos = 256;                  // QOS预取, 默认256

        private ReceiveAckOptions() {
        }

        /**
         * @param autoAck            boolean
         * @param multipleAck        boolean
         * @param maxAckTotal        int
         * @param maxAckReconnection int
         * @param qos                int
         */
        private ReceiveAckOptions(boolean autoAck, boolean multipleAck,
                                  int maxAckTotal, int maxAckReconnection,
                                  int qos) {
            this.autoAck = autoAck;
            this.multipleAck = multipleAck;
            this.maxAckTotal = maxAckTotal;
            this.maxAckReconnection = maxAckReconnection;
            this.qos = qos;
        }

        /**
         * 使用默认参数
         *
         * @return ReceiveAckOptions
         */
        public static ReceiveAckOptions withDefault() {
            return new ReceiveAckOptions();
        }

        /**
         * 指定具体参数
         *
         * @param autoAck            boolean
         * @param multipleAck        boolean
         * @param maxAckTotal        int
         * @param maxAckReconnection int
         * @param qos                int
         * @return ReceiveAckOptions
         */
        public static ReceiveAckOptions with(boolean autoAck, boolean multipleAck,
                                             int maxAckTotal, int maxAckReconnection,
                                             int qos) {
            return new ReceiveAckOptions(autoAck, multipleAck, maxAckTotal,
                maxAckReconnection, qos);
        }

    }

}
