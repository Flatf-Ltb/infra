package io.flatf.transport.rmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.flatf.common.character.Charsets;
import io.flatf.common.lang.Validator;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.thread.Sleep;
import io.flatf.common.thread.Threads;
import io.flatf.common.util.StringSupport;
import io.flatf.transport.api.Publisher;
import io.flatf.transport.api.Sender;
import io.flatf.transport.exception.PublishFailedException;
import io.flatf.transport.rmq.config.RmqConnectionConfig;
import io.flatf.transport.rmq.config.RmqPublisherConfig;
import io.flatf.transport.rmq.declare.ExchangeRelationship;
import io.flatf.transport.rmq.exception.DeclareException;
import io.flatf.transport.rmq.exception.DeclareRuntimeException;
import io.flatf.transport.rmq.exception.NoAckException;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static io.flatf.common.datetime.DateTimeUtil.datetimeOfMillisecond;
import static io.flatf.common.util.StringSupport.nonEmpty;

@ThreadSafe
public class RmqProducer extends RmqTransport implements Publisher<String, byte[]>, Sender<byte[]> {

    private static final Logger LOG = Log4j2LoggerFactory.getLogger(RmqProducer.class);

    // 发布消息使用的[ExchangeDefinition]
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

    private final boolean confirm;
    private final long confirmTimeout;
    private final int confirmRetry;

    private final String publisherName;

    /**
     * @param cfg RmqPublisherConfig
     */
    public RmqProducer(@Nonnull RmqPublisherConfig cfg) {
        this(null, cfg);
    }

    /**
     * @param tag String
     * @param cfg RmqPublisherConfig
     */
    public RmqProducer(@Nullable String tag, @Nonnull RmqPublisherConfig cfg) {
        super(nonEmpty(tag) ? tag : "publisher-" + datetimeOfMillisecond(), cfg.getConnectionConfig());
        Validator.nonNull(cfg.exchange(), "exchangeRelation");
        this.usedExchange = cfg.exchange();
        this.exchangeName = usedExchange.getExchangeName();
        this.defaultRoutingKey = cfg.defaultRoutingKey();
        this.defaultMsgProps = cfg.defaultMsgProps();
        this.msgPropsSupplier = cfg.msgPropsSupplier();
        this.confirm = cfg.confirmOptions().confirm();
        this.confirmTimeout = cfg.confirmOptions().confirmTimeout();
        this.confirmRetry = cfg.confirmOptions().confirmRetry();
        this.hasPropsSupplier = msgPropsSupplier != null;
        this.publisherName = "publisher::" + connectionConfig.connectionInfo() + "$" + exchangeName;
        createConnection();
        declare();
    }

    private void declare() throws DeclareRuntimeException {
        try {
            if (usedExchange == ExchangeRelationship.ANONYMOUS)
                LOG.warn(
                    "Publisher -> {} use anonymous exchange, Please specify [queue name] as the [routing key] when publish",
                    tag);
            else
                this.usedExchange.declare(RmqOperator.with(channel));
        } catch (DeclareException e) {
            // 在定义Exchange和进行绑定时抛出任何异常都需要终止程序
            LOG.error("Exchange declare throw exception -> connection configurator info : {}, " + "error message : {}",
                connectionConfig.getConfigInfo(), e.getMessage(), e);
            closeIgnoreException();
            throw new DeclareRuntimeException(e);
        }
    }

    @Override
    public void send(@Nonnull byte[] msg) throws PublishFailedException {
        publish(msg);
    }

    @Override
    public void publish(@Nonnull byte[] msg) throws PublishFailedException {
        publish(defaultRoutingKey, msg, defaultMsgProps);
    }

    @Override
    public void publish(@Nonnull String target, @Nonnull byte[] msg) throws PublishFailedException {
        publish(target, msg, hasPropsSupplier ? msgPropsSupplier.get() : defaultMsgProps);
    }

    /**
     * @param target String
     * @param msg    byte[]
     * @param props  BasicProperties
     * @throws PublishFailedException pfe
     */
    public void publish(@Nonnull String target, @Nonnull byte[] msg, @Nonnull BasicProperties props)
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
                LOG.error("Func publish isConfirm==[true] throw IOException -> {}, msg==[{}]", e.getMessage(),
                    StringSupport.toString(msg), e);
                closeIgnoreException();
                throw new PublishFailedException(e);
            } catch (NoAckException e) {
                LOG.error("Func publish isConfirm==[true] throw NoConfirmException -> {}, msg==[{}]", e.getMessage(),
                    StringSupport.toString(msg), e);
                throw new PublishFailedException(e);
            }
        } else {
            try {
                basicPublish(target, msg, props);
            } catch (IOException e) {
                LOG.error("Func publish isConfirm==[false] throw IOException -> {}, msg==[{}]", e.getMessage(),
                    StringSupport.toString(msg), e);
                closeIgnoreException();
                throw new PublishFailedException(e);
            }
        }
    }

    /**
     * @param routingKey String
     * @param msg        byte[]
     * @param props      BasicProperties
     * @throws IOException    ioe
     * @throws NoAckException nae
     */
    private void confirmPublish(String routingKey, byte[] msg, BasicProperties props)
        throws IOException, NoAckException {
        confirmPublish0(routingKey, msg, props, 0);
    }

    /**
     * TODO 优化异常处理逻辑
     *
     * @param routingKey String
     * @param msg        byte[]
     * @param props      BasicProperties
     * @param retry      int
     * @throws IOException    ioe
     * @throws NoAckException nae
     */
    private void confirmPublish0(String routingKey, byte[] msg, BasicProperties props, int retry)
        throws IOException, NoAckException {
        try {
            channel.confirmSelect();
            basicPublish(routingKey, msg, props);
            if (channel.waitForConfirms(confirmTimeout))
                return;
            LOG.error("Call func channel.waitForConfirms(confirmTimeout==[{}]) retry==[{}]", confirmTimeout, retry);
            if (++retry == confirmRetry)
                throw new NoAckException(exchangeName, routingKey, retry, confirmTimeout);
            confirmPublish0(routingKey, msg, props, retry);
        } catch (IOException e) {
            LOG.error("Func channel.confirmSelect() throw IOException from publisherName -> {}, routingKey -> {}",
                publisherName, routingKey, e);
            throw e;
        } catch (InterruptedException | TimeoutException e) {
            LOG.error("Func channel.waitForConfirms() throw {} from publisherName -> {}, routingKey -> {}",
                e.getClass().getSimpleName(), publisherName, routingKey, e);
        }
    }

    /**
     * @param routingKey String
     * @param msg        byte[]
     * @param props      BasicProperties
     * @throws IOException ioe
     */
    private void basicPublish(String routingKey, byte[] msg, BasicProperties props) throws IOException {
        try {
            channel.basicPublish(
                // param1: the exchange to publish the message to
                exchangeName,
                // param2: the routing key
                routingKey,
                // param3: other properties for the message - routing headers etc
                props,
                // param4: the message body
                msg);
        } catch (IOException e) {
            StringBuilder sb = new StringBuilder(240);
            props.appendPropertyDebugStringTo(sb);
            LOG.error("Func channel.basicPublish(exchange==[{}], routingKey==[{}], properties==[{}], msg==[...]) "
                      + "throw IOException -> {}", exchangeName, routingKey, sb, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public boolean closeIgnoreException() {
        LOG.info("Call func closeIgnoreException() from Publisher name==[{}]", publisherName);
        return super.closeIgnoreException();
    }

    @Override
    public String getName() {
        return publisherName;
    }

    // TODO 重试计数器
    public static class ResendCounter {

    }

    public static void main(String[] args) {

        RmqConnectionConfig connection = RmqConnectionConfig.with("127.0.0.1", 5672, "guest", "guest").build();

        ExchangeRelationship fanoutExchange = ExchangeRelationship.fanout("fanout-test");

        try (RmqProducer publisher = new RmqProducer(
            RmqPublisherConfig.configuration(connection, fanoutExchange).build())) {
            Threads.startNewThread(() -> {
                int count = 0;
                while (true) {
                    Sleep.millis(5000);
                    publisher.publish(String.valueOf(++count).getBytes(Charsets.UTF8));
                    System.out.println(count);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
