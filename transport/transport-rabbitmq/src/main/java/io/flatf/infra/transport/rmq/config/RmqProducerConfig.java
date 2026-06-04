package io.flatf.infra.transport.rmq.config;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.flatf.infra.serialization.json.JsonWriter;
import io.flatf.infra.transport.rmq.declare.AmqpQueue;
import io.flatf.infra.transport.rmq.declare.ExchangeRelationship;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

import static com.rabbitmq.client.MessageProperties.PERSISTENT_BASIC;
import static io.flatf.common.lang.Validator.nonNull;

/**
 * @author yellow013
 */
@Accessors(fluent = true)
public final class RmqProducerConfig extends RmqConfig {

    // 发布者ExchangeDeclare
    @Getter
    private final ExchangeRelationship exchange;

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
    private RmqProducerConfig(Builder builder) {
        super(builder.connectionConfig);
        this.exchange = builder.exchange;
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
    public static Builder with(@Nonnull String host, int port,
                               @Nonnull String username, @Nonnull String password) {
        return with(RmqConnectionConfig.with(host, port, username, password).build());
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
    public static Builder with(@Nonnull String host, int port,
                               @Nonnull String username, @Nonnull String password,
                               @Nullable String virtualHost) {
        return with(RmqConnectionConfig.with(host, port, username, password, virtualHost).build());
    }

    /**
     * Use Anonymous Exchange
     *
     * @param connection RmqConnection
     * @return Builder
     */
    public static Builder with(@Nonnull RmqConnectionConfig connection) {
        return with(connection, ExchangeRelationship.ANONYMOUS);
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
    public static Builder with(@Nonnull String host, int port,
                               @Nonnull String username, @Nonnull String password,
                               @Nonnull ExchangeRelationship publishExchange) {
        return with(RmqConnectionConfig.with(host, port, username, password).build(), publishExchange);
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
    public static Builder with(@Nonnull String host, int port,
                               @Nonnull String username, @Nonnull String password,
                               @Nullable String virtualHost, @Nonnull ExchangeRelationship publishExchange) {
        return with(RmqConnectionConfig.with(host, port, username, password, virtualHost).build(),
            publishExchange);
    }

    /**
     * Use Specified Exchange
     *
     * @param connectionConfig  RmqConnectionConfig
     * @param publishExchange   ExchangeRelationship
     * @return Builder
     */
    public static Builder with(@Nonnull RmqConnectionConfig connectionConfig,
                               @Nonnull ExchangeRelationship publishExchange) {
        return new Builder(nonNull(connectionConfig, "connectionConfig"),
            nonNull(publishExchange, "publishExchange"));
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
        private final RmqConnectionConfig connectionConfig;

        // 消息发布Exchange和相关绑定
        private final ExchangeRelationship exchange;

        // 消息发布RoutingKey, 默认为空字符串
        private String defaultRoutingKey = "";

        // 默认消息发布参数
        private BasicProperties defaultMsgProps = PERSISTENT_BASIC;

        // 默认消息发布参数提供者
        private Supplier<BasicProperties> msgPropsSupplier = null;

        // 发布确认选项
        private PublishConfirmOptions confirmOptions = PublishConfirmOptions.withDefault();

        /**
         * @param connectionConfig      RmqConnection
         * @param exchange ExchangeRelationship
         */
        private Builder(RmqConnectionConfig connectionConfig, ExchangeRelationship exchange) {
            this.connectionConfig = connectionConfig;
            this.exchange = exchange;
        }

        /**
         * @param confirm boolean
         * @return Builder
         */
        public Builder confirm(boolean confirm) {
            this.confirmOptions.confirm(confirm);
            return this;
        }

        /**
         * @param confirmRetry int
         * @return Builder
         */
        public Builder confirmRetry(int confirmRetry) {
            this.confirmOptions.confirmRetry(confirmRetry);
            return this;
        }

        /**
         * @param confirmTimeout long
         * @return Builder
         */
        public Builder confirmTimeout(long confirmTimeout) {
            this.confirmOptions.confirmTimeout(confirmTimeout);
            return this;
        }

        /**
         * @return RmqPublisherConfig
         */
        public RmqProducerConfig build() {
            return new RmqProducerConfig(this);
        }

    }

    /**
     * @author yellow013
     */
    @Getter
    @Setter
    @Accessors(chain = true, fluent = true)
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
        System.out.println(with(
            RmqConnectionConfig
                .with("localhost", 5672, "user0", "password0")
                .build(),
            ExchangeRelationship
                .direct("TEST")
                .bindingQueues(AmqpQueue.named("TEST_0")))
            .build());
    }

}
