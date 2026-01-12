package io.mercury.transport.rmq.config;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.mercury.serialization.json.JsonWriter;
import io.mercury.transport.rmq.declare.AmqpQueue;
import io.mercury.transport.rmq.declare.ExchangeRelationship;
import lombok.Getter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

import static com.rabbitmq.client.MessageProperties.PERSISTENT_BASIC;
import static io.mercury.common.lang.Validator.nonNull;

/**
 * @author yellow013
 */
public final class RmqPublisherConfig extends RmqConfig {

    // 发布者ExchangeDeclare
    @Getter
    private final ExchangeRelationship publishExchange;

    // 消息发布RoutingKey
    @Getter
    private final String defaultRoutingKey;

    // 消息发布参数
    @Getter
    private final BasicProperties defaultMsgProps;

    // 消息参数提供者
    @Getter
    private final Supplier<BasicProperties> msgPropsSupplier;

    // 发布确认选项
    @Getter
    private final PublishConfirmOptions confirmOptions;

    /**
     * @param builder Builder
     */
    private RmqPublisherConfig(Builder builder) {
        super(builder.connection);
        this.publishExchange = builder.publishExchange;
        this.defaultRoutingKey = builder.defaultRoutingKey;
        this.defaultMsgProps = builder.defaultMsgProps;
        this.msgPropsSupplier = builder.msgPropsSupplier;
        this.confirmOptions = builder.confirmOptions;
    }

    /**
     * Use Anonymous Exchange
     *
     * @param host     String
     * @param port     int
     * @param username String
     * @param password String
     * @return Builder
     */
    public static Builder configuration(@Nonnull String host, int port,
                                        @Nonnull String username, @Nonnull String password) {
        return configuration(RmqConnection.with(host, port, username, password).build());
    }

    /**
     * Use Anonymous Exchange
     *
     * @param host        String
     * @param port        int
     * @param username    String
     * @param password    String
     * @param virtualHost String
     * @return Builder
     */
    public static Builder configuration(@Nonnull String host, int port,
                                        @Nonnull String username, @Nonnull String password,
                                        @Nullable String virtualHost) {
        return configuration(RmqConnection.with(host, port, username, password, virtualHost).build());
    }

    /**
     * Use Anonymous Exchange
     *
     * @param connection RmqConnection
     * @return Builder
     */
    public static Builder configuration(@Nonnull RmqConnection connection) {
        return configuration(connection, ExchangeRelationship.Anonymous);
    }

    /**
     * Use Specified Exchange
     *
     * @param host            String
     * @param port            int
     * @param username        String
     * @param password        String
     * @param publishExchange ExchangeRelationship
     * @return Builder
     */
    public static Builder configuration(@Nonnull String host, int port,
                                        @Nonnull String username, @Nonnull String password,
                                        @Nonnull ExchangeRelationship publishExchange) {
        return configuration(RmqConnection.with(host, port, username, password).build(), publishExchange);
    }

    /**
     * Use Specified Exchange
     *
     * @param host            String
     * @param port            int
     * @param username        String
     * @param password        String
     * @param virtualHost     String
     * @param publishExchange ExchangeRelationship
     * @return Builder
     */
    public static Builder configuration(@Nonnull String host, int port,
                                        @Nonnull String username, @Nonnull String password,
                                        @Nullable String virtualHost, @Nonnull ExchangeRelationship publishExchange) {
        return configuration(RmqConnection.with(host, port, username, password, virtualHost).build(),
                publishExchange);
    }

    /**
     * Use Specified Exchange
     *
     * @param connection      RmqConnection
     * @param publishExchange ExchangeRelationship
     * @return Builder
     */
    public static Builder configuration(@Nonnull RmqConnection connection,
                                        @Nonnull ExchangeRelationship publishExchange) {
        return new Builder(nonNull(connection, "connection"),
                nonNull(publishExchange, "publishExchange"));
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
        
        // 消息发布Exchange和相关绑定
        private final ExchangeRelationship publishExchange;

        // 消息发布RoutingKey, 默认为空字符串
        private String defaultRoutingKey = "";

        // 默认消息发布参数
        private BasicProperties defaultMsgProps = PERSISTENT_BASIC;

        // 默认消息发布参数提供者
        private Supplier<BasicProperties> msgPropsSupplier = null;

        // 发布确认选项
        private PublishConfirmOptions confirmOptions = PublishConfirmOptions.withDefault();

        /**
         * @param connection      RmqConnection
         * @param publishExchange ExchangeRelationship
         */
        private Builder(RmqConnection connection, ExchangeRelationship publishExchange) {
            this.connection = connection;
            this.publishExchange = publishExchange;
        }

        public Builder setDefaultRoutingKey(String defaultRoutingKey) {
            this.defaultRoutingKey = defaultRoutingKey;
            return this;
        }

        public Builder setDefaultMsgProps(BasicProperties defaultMsgProps) {
            this.defaultMsgProps = defaultMsgProps;
            return this;
        }

        public Builder setMsgPropsSupplier(Supplier<BasicProperties> msgPropsSupplier) {
            this.msgPropsSupplier = msgPropsSupplier;
            return this;
        }

        public Builder setConfirmOptions(PublishConfirmOptions confirmOptions) {
            this.confirmOptions = confirmOptions;
            return this;
        }

        /**
         * @param confirm boolean
         * @return Builder
         */
        public Builder setConfirm(boolean confirm) {
            this.confirmOptions.setConfirm(confirm);
            return this;
        }

        /**
         * @param confirmRetry int
         * @return Builder
         */
        public Builder setConfirmRetry(int confirmRetry) {
            this.confirmOptions.setConfirmRetry(confirmRetry);
            return this;
        }

        /**
         * @param confirmTimeout long
         * @return Builder
         */
        public Builder setConfirmTimeout(long confirmTimeout) {
            this.confirmOptions.setConfirmTimeout(confirmTimeout);
            return this;
        }

        /**
         * @return RmqPublisherConfig
         */
        public RmqPublisherConfig build() {
            return new RmqPublisherConfig(this);
        }

    }

    /**
     * @author yellow013
     */
    public static final class PublishConfirmOptions {

        // 是否执行发布确认, 默认false
        private boolean confirm = false;

        // 发布确认超时毫秒数, 默认5000毫秒
        private long confirmTimeout = 5000;

        // 发布确认重试次数, 默认3次
        private int confirmRetry = 3;

        private PublishConfirmOptions() {
        }

        private PublishConfirmOptions(boolean confirm, long confirmTimeout, int confirmRetry) {
            this.confirm = confirm;
            this.confirmTimeout = confirmTimeout;
            this.confirmRetry = confirmRetry;
        }

        public PublishConfirmOptions setConfirm(boolean confirm) {
            this.confirm = confirm;
            return this;
        }

        public PublishConfirmOptions setConfirmTimeout(long confirmTimeout) {
            this.confirmTimeout = confirmTimeout;
            return this;
        }

        public PublishConfirmOptions setConfirmRetry(int confirmRetry) {
            this.confirmRetry = confirmRetry;
            return this;
        }

        public boolean isConfirm() {
            return confirm;
        }

        public long getConfirmTimeout() {
            return confirmTimeout;
        }

        public int getConfirmRetry() {
            return confirmRetry;
        }

        /**
         * 使用默认参数
         *
         * @return PublishConfirmOptions
         */
        public static PublishConfirmOptions withDefault() {
            return new PublishConfirmOptions();
        }

        /**
         * 指定具体参数
         *
         * @return PublishConfirmOptions
         */
        public static PublishConfirmOptions with(boolean confirm, long confirmTimeout, int confirmRetry) {
            return new PublishConfirmOptions(confirm, confirmTimeout, confirmRetry);
        }

    }

    public static void main(String[] args) {
        System.out.println(configuration(
                RmqConnection
                        .with("localhost", 5672, "user0", "password0")
                        .build(),
                ExchangeRelationship
                        .direct("TEST")
                        .bindingQueues(AmqpQueue.named("TEST_0")))
                .build());
    }

}
