package io.flatf.transport.rmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ConfirmCallback;
import io.flatf.common.character.Charsets;
import io.flatf.common.lang.Validator;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.serialization.specific.BytesSerializer;
import io.flatf.common.thread.Sleep;
import io.flatf.common.thread.Threads;
import io.flatf.transport.api.Publisher;
import io.flatf.transport.api.Sender;
import io.flatf.transport.exception.InitializeFailureException;
import io.flatf.transport.exception.PublishFailedException;
import io.flatf.transport.rmq.config.RmqConnection;
import io.flatf.transport.rmq.config.RmqPublisherConfig;
import io.flatf.transport.rmq.declare.ExchangeRelationship;
import io.flatf.transport.rmq.exception.DeclareException;
import io.flatf.transport.rmq.exception.DeclareRuntimeException;
import io.flatf.transport.rmq.exception.MsgConfirmFailureException;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static io.flatf.common.datetime.DateTimeUtil.datetimeOfMillisecond;
import static io.flatf.common.util.StringSupport.nonEmpty;

/**
 * @author yellow013
 * <p>
 * 添加消息序列化器
 */
@ThreadSafe
public class AdvancedRmqPublisher<T> extends RmqTransport implements Publisher<String, T>, Sender<T> {

    private static final Logger log = Log4j2LoggerFactory.getLogger(AdvancedRmqPublisher.class);

    // 发布消息使用的[ExchangeDeclare]
    private final ExchangeRelationship publishExchange;

    // 发布消息使用的[Exchange]
    private final String exchangeName;

    // 发布消息使用的默认[RoutingKey]
    private final String defaultRoutingKey;

    // 发布消息使用的默认[MessageProperties]
    private final BasicProperties defaultMsgProps;

    // [MessageProperties]的提供者
    private final Supplier<BasicProperties> msgPropsSupplier;

    // 是否有[MessageProperties]的提供者
    private final boolean hasPropsSupplier;

    // 是否执行发布确认
    private final boolean confirm;

    // 发布确认超时毫秒数
    private final long confirmTimeout;

    // 发布确认重试次数
    private final int confirmRetry;

    // 发布者名称
    private final String publisherName;

    // 发布消息使用的序列化器
    private final BytesSerializer<T> serializer;

    /**
     * @param <T>        T
     * @param config     RmqPublisherConfig
     * @param serializer BytesSerializer<T>
     * @return AdvancedRmqPublisher<T>
     */
    public static <T> AdvancedRmqPublisher<T> create(@Nonnull RmqPublisherConfig config,
                                                     @Nonnull BytesSerializer<T> serializer) {
        return new AdvancedRmqPublisher<>(null, config, serializer, null, null);
    }

    /**
     * @param <T>        T
     * @param tag        String
     * @param config     RmqPublisherConfig
     * @param serializer BytesSerializer<T>
     * @return AdvancedRmqPublisher<T>
     */
    public static <T> AdvancedRmqPublisher<T> create(@Nullable String tag, @Nonnull RmqPublisherConfig config,
                                                     @Nonnull BytesSerializer<T> serializer) {
        return new AdvancedRmqPublisher<>(tag, config, serializer, null, null);
    }

    /**
     * @param <T>           T
     * @param tag           String
     * @param config        RmqPublisherConfig
     * @param serializer    BytesSerializer<T>
     * @param ackCallback   AckCallback
     * @param noAckCallback NoAckCallback
     * @return AdvancedRmqPublisher<T>
     */
    public static <T> AdvancedRmqPublisher<T> create(@Nullable String tag, @Nonnull RmqPublisherConfig config,
                                                     @Nonnull BytesSerializer<T> serializer,
                                                     @Nonnull AckCallback ackCallback,
                                                     @Nonnull NoAckCallback noAckCallback) {
        return new AdvancedRmqPublisher<>(tag, config, serializer, ackCallback, noAckCallback);
    }

    /**
     * @param config RmqPublisherConfig
     * @return AdvancedRmqPublisher<byte [ ]>
     */
    public static AdvancedRmqPublisher<byte[]> createWithBytes(@Nonnull RmqPublisherConfig config) {
        return createWithBytes(null, config, null, null);
    }

    /**
     * @param tag    String
     * @param config RmqPublisherConfig
     * @return AdvancedRmqPublisher<byte [ ]>
     */
    public static AdvancedRmqPublisher<byte[]> createWithBytes(@Nullable String tag,
                                                               @Nonnull RmqPublisherConfig config) {
        return createWithBytes(tag, config, null, null);
    }

    /**
     * @param tag           String
     * @param config        RmqPublisherConfig
     * @param ackCallback   AckCallback
     * @param noAckCallback NoAckCallback
     * @return AdvancedRmqPublisher<byte [ ]>
     */
    public static AdvancedRmqPublisher<byte[]> createWithBytes(@Nullable String tag,
                                                               @Nonnull RmqPublisherConfig config,
                                                               @Nullable AckCallback ackCallback,
                                                               @Nullable NoAckCallback noAckCallback) {
        return new AdvancedRmqPublisher<>(tag, config, msg -> msg, ackCallback, noAckCallback);
    }

    /**
     * @param config RmqPublisherConfig
     * @return AdvancedRmqPublisher<String>
     */
    public static AdvancedRmqPublisher<String> createWithString(@Nonnull RmqPublisherConfig config) {
        return createWithString(null, config, Charsets.UTF8, null, null);
    }

    /**
     * @param config  RmqPublisherConfig
     * @param charset Charset
     * @return AdvancedRmqPublisher<String>
     */
    public static AdvancedRmqPublisher<String> createWithString(@Nonnull RmqPublisherConfig config,
                                                                @Nonnull Charset charset) {
        return createWithString(null, config, charset, null, null);
    }

    /**
     * @param tag    String
     * @param config RmqPublisherConfig
     * @return AdvancedRmqPublisher<String>
     */
    public static AdvancedRmqPublisher<String> createWithString(@Nullable String tag,
                                                                @Nonnull RmqPublisherConfig config) {
        return createWithString(tag, config, Charsets.UTF8, null, null);
    }

    /**
     * @param tag     String
     * @param config  RmqPublisherConfig
     * @param charset Charset
     * @return AdvancedRmqPublisher<String>
     */
    public static AdvancedRmqPublisher<String> createWithString(@Nullable String tag,
                                                                @Nonnull RmqPublisherConfig config, @Nonnull Charset charset) {
        return createWithString(tag, config, charset, null, null);
    }

    /**
     * @param tag           String
     * @param config        RmqPublisherConfig
     * @param charset       Charset
     * @param ackCallback   AckCallback
     * @param noAckCallback NoAckCallback
     * @return AdvancedRmqPublisher<String>
     */
    public static AdvancedRmqPublisher<String> createWithString(@Nullable String tag,
                                                                @Nonnull RmqPublisherConfig config,
                                                                @Nonnull Charset charset,
                                                                @Nullable AckCallback ackCallback,
                                                                @Nullable NoAckCallback noAckCallback) {
        return new AdvancedRmqPublisher<>(tag, config, msg -> msg != null ? msg.getBytes(charset) : null, ackCallback, noAckCallback);
    }

    /**
     * @param tag           标签
     * @param config        配置器
     * @param serializer    序列化器
     * @param ackCallback   ACK成功回调
     * @param noAckCallback ACK未成功回调
     */
    private AdvancedRmqPublisher(@Nullable String tag, @Nonnull RmqPublisherConfig config,
                                 @Nonnull BytesSerializer<T> serializer, @Nullable AckCallback ackCallback,
                                 @Nullable NoAckCallback noAckCallback) {
        super(nonEmpty(tag) ? tag : "adv-pub-" + datetimeOfMillisecond(), config.getConnection());
        Validator.nonNull(config.getPublishExchange(), "exchangeRelation");
        this.publishExchange = config.getPublishExchange();
        this.exchangeName = publishExchange.getExchangeName();
        this.defaultRoutingKey = config.getDefaultRoutingKey();
        this.defaultMsgProps = config.getDefaultMsgProps();
        this.msgPropsSupplier = config.getMsgPropsSupplier();
        this.confirm = config.getConfirmOptions().isConfirm();
        this.confirmTimeout = config.getConfirmOptions().getConfirmTimeout();
        this.confirmRetry = config.getConfirmOptions().getConfirmRetry();
        this.serializer = serializer;
        this.hasPropsSupplier = msgPropsSupplier != null;
        this.publisherName = "publisher::[" + rmqConnection.getConnectionInfo() + "$" + exchangeName + "]";
        createConnection();
        declareExchange();
        // 如果设置为需要应答确认, 则进行相关设置
        if (confirm) {
            // 是否存在ACK成功回调
            final boolean hasAckCallback = ackCallback != null;
            // 是否存在ACK未成功回调
            final boolean hasNoAckCallback = noAckCallback != null;
            // 添加ACK & NoAck回调
            channel.addConfirmListener(
                    // Ack Callback
                    (deliveryTag, multiple) -> {
                        if (hasAckCallback)
                            ackCallback.handle(deliveryTag, multiple);
                        else
                            log.warn("Undefined AckCallback function. Publisher -> {}", publisherName);
                    },
                    // NoAck Callback
                    (deliveryTag, multiple) -> {
                        if (hasNoAckCallback)
                            noAckCallback.handle(deliveryTag, multiple);
                        else
                            log.warn("Undefined NoAckCallback function. Publisher -> {}", publisherName);
                    });
            try {
                // Enables publisher acknowledgements on this channel.
                channel.confirmSelect();
            } catch (IOException ioe) {
                log.error("Enables publisher acknowledgements failure, publisherName -> {}, connectionInfo -> {}",
                        publisherName, rmqConnection.getConnectionInfo(), ioe);
                throw new InitializeFailureException(
                        "Enables publisher acknowledgements failure, From publisher -> " + publisherName, ioe);
            }
        }
        newStartTime();
    }

    /**
     * 定义相关队列组件
     *
     * @throws DeclareRuntimeException e
     */
    private void declareExchange() throws DeclareRuntimeException {
        try {
            if (publishExchange == ExchangeRelationship.Anonymous) {
                log.warn("Publisher -> {} use anonymous exchange, Please specify [queue name] "
                        + "as the [routing key] when publish", tag);
            } else {
                this.publishExchange.declare(RmqOperator.with(channel));
            }
        } catch (DeclareException e) {
            // 在定义Exchange和进行绑定时抛出任何异常都需要终止程序
            log.error("Exchange declare throw exception -> connection configurator info : {}, " + "error message : {}",
                    rmqConnection.getConfigInfo(), e.getMessage(), e);
            closeIgnoreException();
            throw new DeclareRuntimeException(e);
        }

    }

    @Override
    public void send(@Nonnull T msg) throws PublishFailedException {
        publish(msg);
    }

    @Override
    public void publish(@Nonnull T msg) throws PublishFailedException {
        publish(defaultRoutingKey, msg, defaultMsgProps);
    }

    @Override
    public void publish(@Nonnull String target, @Nonnull T msg) throws PublishFailedException {
        publish(target, msg, hasPropsSupplier ? msgPropsSupplier.get() : defaultMsgProps);
    }

    /**
     * @param target String
     * @param msg    T
     * @param props  BasicProperties
     * @throws PublishFailedException pfe
     */
    public void publish(@Nonnull String target, @Nonnull T msg, @Nonnull BasicProperties props)
            throws PublishFailedException {
        // 记录重试次数
        int retry = 0;
        // 调用isConnected(), 检查channel和connection是否打开, 如果没有打开, 先销毁连接, 再重新创建连接.
        while (!isConnected()) {
            log.error("Detect connection isConnected() == false, retry {}", (++retry));
            closeIgnoreException();
            Sleep.millis(rmqConnection.getRecoveryInterval());
            createConnection();
        }
        if (confirm) {
            try {
                confirmPublish(target, msg, props);
            } catch (IOException e) {
                log.error("Function publish throw IOException -> {}, isConfirm==[true], msg==[{}]", e.getMessage(), msg,
                        e);
                closeIgnoreException();
                throw new PublishFailedException(e);
            } catch (MsgConfirmFailureException e) {
                log.error("Function publish throw NoConfirmException -> {}, isConfirm==[true], msg==[{}]",
                        e.getMessage(), msg, e);
                throw new PublishFailedException(e);
            }
        } else {
            try {
                basicPublish(target, msg, props);
            } catch (IOException e) {
                log.error("Function publish throw IOException -> {}, isConfirm==[false], msg==[{}]", e.getMessage(),
                        msg, e);
                closeIgnoreException();
                throw new PublishFailedException(e);
            }
        }
    }

    /**
     * @param routingKey String
     * @param msg        T
     * @param props      BasicProperties
     * @throws IOException                ioe
     * @throws MsgConfirmFailureException e
     */
    private void confirmPublish(String routingKey, T msg, BasicProperties props)
            throws IOException, MsgConfirmFailureException {
        confirmPublish0(routingKey, msg, props, 0);
    }

    /**
     * TODO 优化异常处理逻辑
     *
     * @param routingKey String
     * @param msg        T
     * @param props      BasicProperties
     * @param retry      int
     * @throws IOException                ioe
     * @throws MsgConfirmFailureException e
     */
    private void confirmPublish0(String routingKey, T msg, BasicProperties props, int retry)
            throws IOException, MsgConfirmFailureException {
        try {
            basicPublish(routingKey, msg, props);
            // 启用发布确认
            if (channel.waitForConfirms(confirmTimeout)) {
                return;
            }
            log.error("Call method channel.waitForConfirms(confirmTimeout==[{}]) retry==[{}]", confirmTimeout, retry);
            if (++retry == confirmRetry) {
                throw new MsgConfirmFailureException(exchangeName, routingKey, retry, confirmTimeout);
            }
            confirmPublish0(routingKey, msg, props, retry);
        } catch (IOException e) {
            log.error("Function basicPublish() throw IOException from publisherName -> {}, routingKey -> {}",
                    publisherName, routingKey, e);
            throw e;
        } catch (InterruptedException | TimeoutException e) {
            log.error("Function channel.waitForConfirms() throw {} from publisherName -> {}, routingKey -> {}",
                    e.getClass().getSimpleName(), publisherName, routingKey, e);
        }
    }

    /**
     * @param routingKey String
     * @param msg        T
     * @param props      BasicProperties
     * @throws IOException ioe
     */
    private void basicPublish(String routingKey, T msg, BasicProperties props) throws IOException {
        try {
            // TODO 添加序列化异常处理
            byte[] bytes = serializer.serialize(msg);
            if (bytes != null) {
                channel.basicPublish(
                        // exchange: the exchange to publish the message to
                        exchangeName,
                        // routingKey: the routing key
                        routingKey,
                        // props: other properties for the message - routing headers etc
                        props,
                        // body: the message body
                        bytes);
            }
        } catch (IOException ioe) {
            StringBuilder properties = new StringBuilder(500);
            props.appendPropertyDebugStringTo(properties);
            log.error(
                    "Function channel.basicPublish() throw IOException, exchange==[{}], routingKey==[{}], properties==[{}] -> {}",
                    exchangeName, routingKey, properties, ioe.getMessage(), ioe);
            throw ioe;
        }
    }

    @Override
    public boolean closeIgnoreException() {
        log.info("Call method destroy() from Publisher name==[{}]", publisherName);
        return super.closeIgnoreException();
    }

    @Override
    public String getName() {
        return publisherName;
    }

    // TODO 重试计数器
    public static class ResendCounter {

    }

    @FunctionalInterface
    public interface AckCallback extends ConfirmCallback {

    }

    @FunctionalInterface
    public interface NoAckCallback extends ConfirmCallback {

    }

    public static void main(String[] args) {

        RmqConnection connection = RmqConnection.with("127.0.0.1", 5672, "guest", "guest").build();

        ExchangeRelationship fanout = ExchangeRelationship.fanout("fanout-test");

        try (AdvancedRmqPublisher<String> publisher = AdvancedRmqPublisher
                .createWithString(RmqPublisherConfig.configuration(connection, fanout).build())) {
            Threads.startNewThread(() -> {
                int count = 0;
                while (true) {
                    Sleep.millis(5000);
                    publisher.publish(String.valueOf(++count));
                    System.out.println(count);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
