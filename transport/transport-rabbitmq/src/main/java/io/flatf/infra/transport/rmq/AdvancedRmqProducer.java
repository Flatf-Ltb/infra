package io.flatf.infra.transport.rmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ConfirmCallback;
import io.flatf.common.character.Charsets;
import io.flatf.common.lang.Validator;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.serialization.specific.BytesSerializer;
import io.flatf.common.thread.Sleep;
import io.flatf.common.thread.Threads;
import io.flatf.foundation.transport.api.Publisher;
import io.flatf.foundation.transport.exception.InitializeFailureException;
import io.flatf.foundation.transport.exception.PublishFailedException;
import io.flatf.infra.transport.rmq.config.RmqConnectionConfig;
import io.flatf.infra.transport.rmq.config.RmqProducerConfig;
import io.flatf.infra.transport.rmq.declare.ExchangeRelationship;
import io.flatf.infra.transport.rmq.exception.DeclareException;
import io.flatf.infra.transport.rmq.exception.DeclareRuntimeException;
import io.flatf.infra.transport.rmq.exception.MsgConfirmFailureException;
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
public class AdvancedRmqProducer<T> extends RmqTransport implements Publisher<String, T> {

    private static final Logger LOG = Log4j2LoggerFactory.getLogger(AdvancedRmqProducer.class);

    // 发布消息使用的[ExchangeDeclare]
    private final ExchangeRelationship usedExchange;

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
     * @return AdvancedRmqProducer<T>
     */
    public static <T> AdvancedRmqProducer<T> create(@Nonnull RmqProducerConfig config,
                                                    @Nonnull BytesSerializer<T> serializer) {
        return new AdvancedRmqProducer<>(null, config, serializer, null, null);
    }

    /**
     * @param <T>        T
     * @param tag        String
     * @param config     RmqPublisherConfig
     * @param serializer BytesSerializer<T>
     * @return AdvancedRmqProducer<T>
     */
    public static <T> AdvancedRmqProducer<T> create(@Nullable String tag, @Nonnull RmqProducerConfig config,
                                                    @Nonnull BytesSerializer<T> serializer) {
        return new AdvancedRmqProducer<>(tag, config, serializer, null, null);
    }

    /**
     * @param <T>           T
     * @param tag           String
     * @param config        RmqPublisherConfig
     * @param serializer    BytesSerializer<T>
     * @param ackCallback   AckCallback
     * @param noAckCallback NoAckCallback
     * @return AdvancedRmqProducer<T>
     */
    public static <T> AdvancedRmqProducer<T> create(@Nullable String tag, @Nonnull RmqProducerConfig config,
                                                    @Nonnull BytesSerializer<T> serializer,
                                                    @Nonnull AckCallback ackCallback,
                                                    @Nonnull NoAckCallback noAckCallback) {
        return new AdvancedRmqProducer<>(tag, config, serializer, ackCallback, noAckCallback);
    }

    /**
     * @param config RmqPublisherConfig
     * @return AdvancedRmqProducer<byte[]>
     */
    public static AdvancedRmqProducer<byte[]> createWithBytes(@Nonnull RmqProducerConfig config) {
        return createWithBytes(null, config, null, null);
    }

    /**
     * @param tag    String
     * @param config RmqPublisherConfig
     * @return AdvancedRmqProducer<byte[]>
     */
    public static AdvancedRmqProducer<byte[]> createWithBytes(@Nullable String tag,
                                                              @Nonnull RmqProducerConfig config) {
        return createWithBytes(tag, config, null, null);
    }

    /**
     * @param tag           String
     * @param config        RmqPublisherConfig
     * @param ackCallback   AckCallback
     * @param noAckCallback NoAckCallback
     * @return AdvancedRmqProducer<byte[]>
     */
    public static AdvancedRmqProducer<byte[]> createWithBytes(@Nullable String tag,
                                                              @Nonnull RmqProducerConfig config,
                                                              @Nullable AckCallback ackCallback,
                                                              @Nullable NoAckCallback noAckCallback) {
        return new AdvancedRmqProducer<>(tag, config, msg -> msg, ackCallback, noAckCallback);
    }

    /**
     * @param config RmqPublisherConfig
     * @return AdvancedRmqProducer<String>
     */
    public static AdvancedRmqProducer<String> createWithString(@Nonnull RmqProducerConfig config) {
        return createWithString(null, config, Charsets.UTF8, null, null);
    }

    /**
     * @param config  RmqPublisherConfig
     * @param charset Charset
     * @return AdvancedRmqProducer<String>
     */
    public static AdvancedRmqProducer<String> createWithString(@Nonnull RmqProducerConfig config,
                                                               @Nonnull Charset charset) {
        return createWithString(null, config, charset, null, null);
    }

    /**
     * @param tag    String
     * @param config RmqPublisherConfig
     * @return AdvancedRmqProducer<String>
     */
    public static AdvancedRmqProducer<String> createWithString(@Nullable String tag,
                                                               @Nonnull RmqProducerConfig config) {
        return createWithString(tag, config, Charsets.UTF8, null, null);
    }

    /**
     * @param tag     String
     * @param config  RmqPublisherConfig
     * @param charset Charset
     * @return AdvancedRmqProducer<String>
     */
    public static AdvancedRmqProducer<String> createWithString(@Nullable String tag,
                                                               @Nonnull RmqProducerConfig config, @Nonnull Charset charset) {
        return createWithString(tag, config, charset, null, null);
    }

    /**
     * @param tag           String
     * @param config        RmqPublisherConfig
     * @param charset       Charset
     * @param ackCallback   AckCallback
     * @param noAckCallback NoAckCallback
     * @return AdvancedRmqProducer<String>
     */
    public static AdvancedRmqProducer<String> createWithString(@Nullable String tag,
                                                               @Nonnull RmqProducerConfig config,
                                                               @Nonnull Charset charset,
                                                               @Nullable AckCallback ackCallback,
                                                               @Nullable NoAckCallback noAckCallback) {
        return new AdvancedRmqProducer<>(tag, config, msg -> msg != null ? msg.getBytes(charset) : null, ackCallback, noAckCallback);
    }

    /**
     * @param tag           标签
     * @param config        配置器
     * @param serializer    序列化器
     * @param ackCallback   ACK成功回调
     * @param noAckCallback ACK未成功回调
     */
    private AdvancedRmqProducer(@Nullable String tag, @Nonnull RmqProducerConfig config,
                                @Nonnull BytesSerializer<T> serializer, @Nullable AckCallback ackCallback,
                                @Nullable NoAckCallback noAckCallback) {
        super(nonEmpty(tag) ? tag : "adv-pub-" + datetimeOfMillisecond(), config.getConnectionConfig());
        Validator.nonNull(config.exchange(), "exchange");
        this.usedExchange = config.exchange();
        this.exchangeName = usedExchange.getExchangeName();
        this.defaultRoutingKey = config.defaultRoutingKey();
        this.defaultMsgProps = config.defaultMsgProps();
        this.msgPropsSupplier = config.msgPropsSupplier();
        this.confirm = config.confirmOptions().confirm();
        this.confirmTimeout = config.confirmOptions().confirmTimeout();
        this.confirmRetry = config.confirmOptions().confirmRetry();
        this.serializer = serializer;
        this.hasPropsSupplier = msgPropsSupplier != null;
        this.publisherName = "publisher::[" + connectionConfig.connectionInfo() + "$" + exchangeName + "]";
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
                        LOG.warn("Undefined AckCallback function. Publisher -> {}", publisherName);
                },
                // NoAck Callback
                (deliveryTag, multiple) -> {
                    if (hasNoAckCallback)
                        noAckCallback.handle(deliveryTag, multiple);
                    else
                        LOG.warn("Undefined NoAckCallback function. Publisher -> {}", publisherName);
                });
            try {
                // Enables publisher acknowledgements on this channel.
                channel.confirmSelect();
            } catch (IOException ioe) {
                LOG.error("Enables publisher acknowledgements failure, publisherName -> {}, connectionInfo -> {}",
                    publisherName, connectionConfig.connectionInfo(), ioe);
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
            if (usedExchange == ExchangeRelationship.ANONYMOUS) {
                LOG.warn("Publisher -> {} use anonymous exchange, Please specify [queue name] "
                         + "as the [routing key] when publish", tag);
            } else {
                this.usedExchange.declare(RmqOperator.with(channel));
            }
        } catch (DeclareException e) {
            // 在定义Exchange和进行绑定时抛出任何异常都需要终止程序
            LOG.error("Exchange declare throw exception -> connection configurator info : {}, " + "error message : {}",
                connectionConfig.getConfigInfo(), e.getMessage(), e);
            closeIgnoreException();
            throw new DeclareRuntimeException(e);
        }

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
            LOG.error("Detect connection isConnected() == false, retry {}", (++retry));
            closeIgnoreException();
            Sleep.millis(connectionConfig.recoveryInterval());
            createConnection();
        }
        if (confirm) {
            try {
                confirmPublish(target, msg, props);
            } catch (IOException e) {
                LOG.error("Function publish throw IOException -> {}, isConfirm==[true], msg==[{}]", e.getMessage(), msg,
                    e);
                closeIgnoreException();
                throw new PublishFailedException(e);
            } catch (MsgConfirmFailureException e) {
                LOG.error("Function publish throw NoConfirmException -> {}, isConfirm==[true], msg==[{}]",
                    e.getMessage(), msg, e);
                throw new PublishFailedException(e);
            }
        } else {
            try {
                basicPublish(target, msg, props);
            } catch (IOException e) {
                LOG.error("Function publish throw IOException -> {}, isConfirm==[false], msg==[{}]", e.getMessage(),
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
            LOG.error("Call method channel.waitForConfirms(confirmTimeout==[{}]) retry==[{}]", confirmTimeout, retry);
            if (++retry == confirmRetry) {
                throw new MsgConfirmFailureException(exchangeName, routingKey, retry, confirmTimeout);
            }
            confirmPublish0(routingKey, msg, props, retry);
        } catch (IOException e) {
            LOG.error("Function basicPublish() throw IOException from publisherName -> {}, routingKey -> {}",
                publisherName, routingKey, e);
            throw e;
        } catch (InterruptedException | TimeoutException e) {
            LOG.error("Function channel.waitForConfirms() throw {} from publisherName -> {}, routingKey -> {}",
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
            LOG.error(
                "Function channel.basicPublish() throw IOException, exchange==[{}], routingKey==[{}], properties==[{}] -> {}",
                exchangeName, routingKey, properties, ioe.getMessage(), ioe);
            throw ioe;
        }
    }

    @Override
    public boolean closeIgnoreException() {
        LOG.info("Call method destroy() from Publisher name==[{}]", publisherName);
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

        RmqConnectionConfig connection = RmqConnectionConfig.with("127.0.0.1", 5672, "guest", "guest").build();

        ExchangeRelationship fanout = ExchangeRelationship.fanout("fanout-test");

        try (AdvancedRmqProducer<String> publisher = AdvancedRmqProducer
            .createWithString(RmqProducerConfig.with(connection, fanout).build())) {
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
