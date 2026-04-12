package io.flatf.transport.rmq.config;

import io.flatf.serialization.json.JsonWriter;
import io.flatf.transport.rmq.declare.ExchangeRelationship;
import io.flatf.transport.rmq.declare.QueueRelationship;

import javax.annotation.Nonnull;
import java.util.Map;

import static io.flatf.common.lang.Validator.nonNull;

/**
 * @author yellow013
 */
public final class RmqReceiverConfig extends RmqConfig {

    // 接受者QueueDeclare
    private final QueueRelationship receiveQueue;

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
        this.receiveQueue = builder.receiveQueue;
        this.errMsgExchange = builder.errMsgExchange;
        this.errMsgRoutingKey = builder.errMsgRoutingKey;
        this.errMsgQueue = builder.errMsgQueue;
        this.exclusive = builder.exclusive;
        this.ackOptions = builder.ackOptions;
        this.args = builder.args;
    }

    /**
     * @param connection   RmqConnection
     * @param receiveQueue QueueRelationship
     * @return Builder
     */
    public static Builder configuration(@Nonnull RmqConnection connection, @Nonnull QueueRelationship receiveQueue) {
        nonNull(connection, "connection");
        nonNull(receiveQueue, "receiveQueue");
        return new Builder(connection, receiveQueue);
    }

    public QueueRelationship getReceiveQueue() {
        return receiveQueue;
    }

    public ExchangeRelationship getErrMsgExchange() {
        return errMsgExchange;
    }

    public String getErrMsgRoutingKey() {
        return errMsgRoutingKey;
    }

    public QueueRelationship getErrMsgQueue() {
        return errMsgQueue;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public ReceiveAckOptions getAckOptions() {
        return ackOptions;
    }

    public String getToStringCache() {
        return toStringCache;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

    private transient String toStringCache;

    @Override
    public String toString() {
        if (toStringCache == null)
            toStringCache = JsonWriter.toJson(this);
        return toStringCache;
    }

    public static class Builder {

        // 连接配置
        private final RmqConnection connection;
        // 接受者QueueRelationship
        private final QueueRelationship receiveQueue;
        // 错误消息ExchangeRelationship

        /* v UnProcessable Message v */
        // 错误消息处理Exchange和关联关系
        private ExchangeRelationship errMsgExchange;
        // 错误消息处理RoutingKey
        private String errMsgRoutingKey = "";
        // 错误消息处理QueueRelationship和关联关系
        private QueueRelationship errMsgQueue;
        /* ^ UnProcessable Message ^ */

        // 接收者是否独占队列
        private boolean exclusive = false;

        // ACK选项
        private ReceiveAckOptions ackOptions = ReceiveAckOptions.withDefault();

        // 消费者参数, 默认为null
        private Map<String, Object> args = null;

        private Builder(RmqConnection connection, QueueRelationship receiveQueue) {
            this.connection = connection;
            this.receiveQueue = receiveQueue;
        }

        public Builder setErrMsgExchange(ExchangeRelationship errMsgExchange) {
            this.errMsgExchange = errMsgExchange;
            return this;
        }

        public Builder setErrMsgRoutingKey(String errMsgRoutingKey) {
            this.errMsgRoutingKey = errMsgRoutingKey;
            return this;
        }

        public Builder setErrMsgQueue(QueueRelationship errMsgQueue) {
            this.errMsgQueue = errMsgQueue;
            return this;
        }

        public Builder setExclusive(boolean exclusive) {
            this.exclusive = exclusive;
            return this;
        }

        public Builder setAckOptions(ReceiveAckOptions ackOptions) {
            this.ackOptions = ackOptions;
            return this;
        }

        /**
         * @param autoAck the isAutoAck to set
         */
        public Builder setAutoAck(boolean autoAck) {
            this.ackOptions.setAutoAck(autoAck);
            return this;
        }

        /**
         * @param multipleAck the isMultipleAck to set
         */
        public Builder setMultipleAck(boolean multipleAck) {
            this.ackOptions.setMultipleAck(multipleAck);
            return this;
        }

        /**
         * @param maxAckTotal int
         * @return Builder
         */
        public Builder setMaxAckTotal(int maxAckTotal) {
            this.ackOptions.setMaxAckTotal(maxAckTotal);
            return this;
        }

        /**
         * @param maxAckReconnection int
         * @return Builder
         */
        public Builder setMaxAckReconnection(int maxAckReconnection) {
            this.ackOptions.setMaxAckReconnection(maxAckReconnection);
            return this;
        }

        /**
         * @param qos int
         * @return Builder
         */
        public Builder setQos(int qos) {
            this.ackOptions.setQos(qos);
            return this;
        }

        public void setArgs(Map<String, Object> args) {
            this.args = args;
        }

        public RmqReceiverConfig build() {
            return new RmqReceiverConfig(this);
        }

    }

    /**
     * @author yellow013
     */
    public static final class ReceiveAckOptions {

        // 自动ACK, 默认true
        private boolean autoAck = true;

        // 一次ACK多条, 默认false
        private boolean multipleAck = false;

        // ACK最大自动重试次数, 默认16次
        private int maxAckTotal = 16;

        // ACK最大自动重连次数, 默认8次
        private int maxAckReconnection = 8;

        // QOS预取, 默认256
        private int qos = 256;

        /**
         *
         */
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

        public ReceiveAckOptions setAutoAck(boolean autoAck) {
            this.autoAck = autoAck;
            return this;
        }

        public ReceiveAckOptions setMultipleAck(boolean multipleAck) {
            this.multipleAck = multipleAck;
            return this;
        }

        public ReceiveAckOptions setMaxAckTotal(int maxAckTotal) {
            this.maxAckTotal = maxAckTotal;
            return this;
        }

        public ReceiveAckOptions setMaxAckReconnection(int maxAckReconnection) {
            this.maxAckReconnection = maxAckReconnection;
            return this;
        }

        public ReceiveAckOptions setQos(int qos) {
            this.qos = qos;
            return this;
        }

        public boolean isAutoAck() {
            return autoAck;
        }

        public boolean isMultipleAck() {
            return multipleAck;
        }

        public int getMaxAckTotal() {
            return maxAckTotal;
        }

        public int getMaxAckReconnection() {
            return maxAckReconnection;
        }

        public int getQos() {
            return qos;
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
