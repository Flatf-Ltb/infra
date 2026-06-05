package io.flatf.infra.transport.rmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.Envelope;
import io.flatf.common.codec.DecodeException;
import io.flatf.common.functional.Processor;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.infra.serialization.specific.BytesDeserializer;
import io.flatf.common.util.StringSupport;
import io.flatf.infra.transport.api.Receiver;
import io.flatf.infra.transport.exception.ConnectionBreakException;
import io.flatf.infra.transport.exception.ConnectionFailedException;
import io.flatf.infra.transport.exception.ReceiverStartException;
import io.flatf.infra.transport.rmq.config.RmqConsumerConfig;
import io.flatf.infra.transport.rmq.declare.ExchangeRelationship;
import io.flatf.infra.transport.rmq.declare.QueueRelationship;
import io.flatf.infra.transport.rmq.exception.DeclareException;
import io.flatf.infra.transport.rmq.exception.DeclareRuntimeException;
import io.flatf.infra.transport.rmq.exception.MsgHandleException;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

import static io.flatf.common.datetime.DateTimeUtil.datetimeOfMillisecond;
import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.util.StringSupport.nonEmpty;

/**
 * @author yellow013<br>
 * <p>
 * [已完成]改造升级, 使用共同的构建者建立Exchange, RoutingKey, Queue的绑定关系
 */
public class AdvancedRmqConsumer<T> extends RmqTransport implements Receiver, Runnable {

    private static final Logger LOG = Log4j2LoggerFactory.getLogger(AdvancedRmqConsumer.class);

    // 接收消息使用的反序列化器
    private final BytesDeserializer<T> deserializer;

    // 接收消息时使用的回调函数
    private final Processor<T> processor;

    // 接受消费全部消息内容, 包括[consumerTag, 信封, 消息体, 参数]
    private final SelfAckConsumer<T> selfAckConsumer;

    // 接受者QueueDeclare
    private final QueueRelationship usedQueue;

    // 接受者QueueName
    private final String queueName;

    // 消息无法处理时发送到的错误消息ExchangeDeclare
    private final ExchangeRelationship errMsgExchange;

    // 是否有错误消息Exchange
    private boolean hasErrMsgExchange;

    // 消息无法处理时发送到的错误消息Exchange
    private String errMsgExchangeName;

    // 消息无法处理时发送到的错误消息Exchange使用的RoutingKey
    private final String errMsgRoutingKey;

    // 消息无法处理时发送到的错误消息QueueDeclare
    private final QueueRelationship errMsgQueue;

    // 是否有错误消息Queue
    private boolean hasErrMsgQueue;

    // 消息无法处理时发送到的错误消息Queue
    private String errMsgQueueName;

    // 自动ACK
    private final boolean autoAck;

    // 一次ACK多条
    private final boolean multipleAck;

    // ACK最大自动重试次数
    private final int maxAckTotal;

    // ACK最大自动重连次数
    private final int maxAckReconnection;

    // QOS预取值
    private final int qos;

    // Receiver名称
    private final String receiverName;

    // 消费者独占队列
    private final boolean exclusive;

    // 消费者参数, 默认为null
    private final Map<String, Object> args;

    // 应答代理, 用于在外部回调中控制ACK过程
    private AckDelegate ackDelegate;

    /**
     * @param config    RmqReceiverConfig
     * @param processor Processor<byte[]>
     * @return AdvancedRmqConsumer<byte[]>
     */
    public static AdvancedRmqConsumer<byte[]> create(@Nonnull RmqConsumerConfig config,
                                                     @Nonnull Processor<byte[]> processor) {
        return create(null, config, processor);
    }

    /**
     * @param tag       String
     * @param config    RmqReceiverConfig
     * @param processor Processor<byte[]>
     * @return AdvancedRmqConsumer<byte[]>
     */
    public static AdvancedRmqConsumer<byte[]> create(String tag,
                                                     @Nonnull RmqConsumerConfig config,
                                                     @Nonnull Processor<byte[]> processor) {
        return create(tag, config, (msg, reuse) -> msg, processor);
    }

    /**
     * @param <T>          T
     * @param tag          String
     * @param config       RmqReceiverConfig
     * @param deserializer BytesDeserializer<T>
     * @param processor    Processor<T>
     * @return AdvancedRmqConsumer<T>
     */
    public static <T> AdvancedRmqConsumer<T> create(String tag,
                                                    @Nonnull RmqConsumerConfig config,
                                                    @Nonnull BytesDeserializer<T> deserializer,
                                                    @Nonnull Processor<T> processor) {
        nonNull(config, "config");
        nonNull(deserializer, "deserializer");
        nonNull(processor, "processor");
        return new AdvancedRmqConsumer<>(tag, config, deserializer, processor,
            null);
    }

    /**
     * @param config      RmqReceiverConfig
     * @param ackConsumer SelfAckConsumer<byte[]>
     * @return AdvancedRmqConsumer<byte[]>
     */
    public static AdvancedRmqConsumer<byte[]> createWithSelfAck(@Nonnull RmqConsumerConfig config,
                                                                @Nonnull SelfAckConsumer<byte[]> ackConsumer) {
        nonNull(config, "config");
        nonNull(ackConsumer, "ackConsumer");
        return new AdvancedRmqConsumer<>(null, config, (msg, reuse) -> msg,
            null, ackConsumer);
    }

    /**
     * @param tag         String
     * @param config      RmqReceiverConfig
     * @param ackConsumer SelfAckConsumer<byte[]>
     * @return AdvancedRmqConsumer<byte[]>
     */
    public static AdvancedRmqConsumer<byte[]> createWithSelfAck(String tag,
                                                                @Nonnull RmqConsumerConfig config,
                                                                @Nonnull SelfAckConsumer<byte[]> ackConsumer) {
        nonNull(config, "config");
        nonNull(ackConsumer, "ackConsumer");
        return new AdvancedRmqConsumer<>(tag, config, (msg, reuse) -> msg,
            null, ackConsumer);
    }

    /**
     * @param <T>          T
     * @param tag          String
     * @param config       RmqReceiverConfig
     * @param deserializer BytesDeserializer<T>
     * @param ackConsumer  SelfAckConsumer<T>
     * @return AdvancedRmqConsumer<T>
     */
    public static <T> AdvancedRmqConsumer<T> createWithSelfAck(String tag,
                                                               @Nonnull RmqConsumerConfig config,
                                                               @Nonnull BytesDeserializer<T> deserializer,
                                                               @Nonnull SelfAckConsumer<T> ackConsumer) {
        nonNull(config, "config");
        nonNull(deserializer, "deserializer");
        nonNull(ackConsumer, "ackConsumer");
        return new AdvancedRmqConsumer<>(tag, config, deserializer, null, ackConsumer);
    }

    /**
     * @param tag          String
     * @param config       RmqReceiverConfig
     * @param deserializer BytesDeserializer<T>
     * @param processor    Processor<T>
     * @param selfAckConsumer  SelfAckConsumer<T>
     */
    private AdvancedRmqConsumer(String tag,
                                @Nonnull RmqConsumerConfig config,
                                @Nonnull BytesDeserializer<T> deserializer,
                                @Nullable Processor<T> processor,
                                @Nullable SelfAckConsumer<T> selfAckConsumer)
        throws ConnectionFailedException {
        super(nonEmpty(tag) ? tag : "adv-recv-" + datetimeOfMillisecond(), config.getConnectionConfig());
        if (processor == null && selfAckConsumer == null)
            throw new NullPointerException("[Consumer] and [SelfAckConsumer] cannot all be null");

        this.usedQueue = config.queue();
        this.queueName = usedQueue.queueName();
        this.deserializer = deserializer;
        this.errMsgExchange = config.errMsgExchange();
        this.errMsgRoutingKey = config.errMsgRoutingKey();
        this.errMsgQueue = config.errMsgQueue();
        this.autoAck = config.ackOptions().autoAck();
        this.multipleAck = config.ackOptions().multipleAck();
        this.maxAckTotal = config.ackOptions().maxAckTotal();
        this.maxAckReconnection = config.ackOptions().maxAckReconnection();
        this.qos = config.ackOptions().qos();
        this.exclusive = config.exclusive();
        this.args = config.args();
        this.processor = processor;
        this.selfAckConsumer = selfAckConsumer;
        this.receiverName = "adv-recv::[" + connectionConfig.connectionInfo() + "$" + queueName + "]";
        createConnection();
        declareQueue();
        if (selfAckConsumer != null) {
            createAckDelegate();
        }
    }

    /**
     * 创建ACK委托
     */
    private void createAckDelegate() {
        this.ackDelegate = new AckDelegate(this);
    }

    /**
     * 定义相关队列组件
     *
     * @throws DeclareRuntimeException e
     */
    private void declareQueue() throws DeclareRuntimeException {
        RmqOperator operator = RmqOperator.with(channel);
        try {
            this.usedQueue.declare(operator);
        } catch (DeclareException e) {
            LOG.error("Queue declare throw exception -> connection info : {}, error message : {}",
                connectionConfig.getConfigInfo(), e.getMessage(), e);
            // 在定义Queue和进行绑定时抛出任何异常都需要终止程序
            closeIgnoreException();
            throw new DeclareRuntimeException(e);
        }
        if (errMsgExchange != null && errMsgQueue != null) {
            errMsgExchange.bindingQueues(errMsgQueue.getQueue());
            declareErrMsgExchange(operator);
        } else if (errMsgExchange != null) {
            declareErrMsgExchange(operator);
        } else if (errMsgQueue != null) {
            declareErrMsgQueueName(operator);
        }
    }

    private void declareErrMsgExchange(RmqOperator operator) {
        try {
            this.errMsgExchange.declare(operator);
        } catch (DeclareException e) {
            LOG.error(
                "ErrorMsgExchange declare throw exception -> connection configurator info : {}, error message : {}",
                connectionConfig.getConfigInfo(), e.getMessage(), e);
            // 在定义Queue和进行绑定时抛出任何异常都需要终止程序
            closeIgnoreException();
            throw new DeclareRuntimeException(e);
        }
        this.errMsgExchangeName = errMsgExchange.getExchangeName();
        this.hasErrMsgExchange = true;
    }

    private void declareErrMsgQueueName(RmqOperator operator) {
        try {
            this.errMsgQueue.declare(operator);
        } catch (DeclareException e) {
            LOG.error("ErrorMsgQueue declare throw exception -> connection configurator info : {}, error message : {}",
                connectionConfig.getConfigInfo(), e.getMessage(), e);
            // 在定义Queue和进行绑定时抛出任何异常都需要终止程序
            closeIgnoreException();
            throw new DeclareRuntimeException(e);
        }
        this.errMsgQueueName = errMsgQueue.queueName();
        this.hasErrMsgQueue = true;
    }

    @Override
    public void run() {
        receive();
    }

    @Override
    public void receive() {
        nonNull(deserializer, "deserializer");
        // 检测Consumer或SelfAckConsumer, 必须有一个不为null
        if (processor == null && selfAckConsumer == null) {
            throw new NullPointerException("[Consumer] or [SelfAckConsumer] cannot be all null");
        }
        // 如果[autoAck]为[false], 设置QOS数值
        if (!autoAck) {
            // # Set QOS parameter start *****
            try {
                channel.basicQos(qos);
            } catch (IOException e) {
                LOG.error("Function basicQos() qos==[{}] throw IOException message -> {}", qos, e.getMessage(), e);
                throw new ReceiverStartException(e.getMessage(), e);
            }
            // # Set QOS parameter end *****
        }
        // 如果[selfAckConsumer]不为null, 设置selfAckConsumer
        if (selfAckConsumer != null) {
            // # Set SelfAckConsumer start *****
            try {
                channel.basicConsume(
                    // queue: the name of the queue
                    queueName,
                    // autoAck:
                    // true if the server should consider messages acknowledged once delivered;
                    // false if the server should expect explicit acknowledgements
                    autoAck,
                    // consumerTag: consumerTag a client-generated consumer tag to establish context
                    tag,
                    // noLocal:
                    // true if the server should not deliver to this consumer messages published on
                    // this channel's connection.
                    // Note! that the RabbitMQ server does not support this flag.
                    false,
                    // exclusive: true if this is an exclusive consumer.
                    exclusive,
                    // arguments: a set of arguments for to consume.
                    args,
                    // deliverCallback: callback when a message is delivered.
                    (consumerTag, delivery) -> {
                        // 消息处理开始
                        Envelope envelope = delivery.getEnvelope();
                        BasicProperties properties = delivery.getProperties();
                        byte[] body = delivery.getBody();
                        try {
                            LOG.debug("Message [{}] handle start", envelope.getDeliveryTag());
                            LOG.debug(
                                "Callback handleDelivery, consumerTag==[{}], deliveryTag==[{}], body.length==[{}]",
                                consumerTag, envelope.getDeliveryTag(), body.length);
                            T t;
                            try {
                                t = deserializer.deserialization(body);
                            } catch (Exception e) {
                                throw new DecodeException(e);
                            }
                            selfAckConsumer.handleMessage(
                                // 传入ACK委托者
                                ackDelegate,
                                // 传入消费者标识
                                consumerTag,
                                // 包装Message对象
                                new Message<>(envelope, properties, t));
                        } catch (Exception e) {
                            LOG.error("SelfAckConsumer accept msg==[{}] throw Exception -> {}",
                                StringSupport.toString(body), e.getMessage(), e);
                            dumpUnprocessableMsg(e, consumerTag, envelope, properties, body);
                        }
                        if (!autoAck) {
                            if (ack(envelope.getDeliveryTag())) {
                                LOG.debug("Message handle and ack finished");
                            } else {
                                LOG.info("Ack failure envelope.getDeliveryTag()==[{}], Reject message", envelope.getDeliveryTag());
                                channel.basicReject(envelope.getDeliveryTag(), true);
                            }
                        }
                        // 消息处理结束
                        LOG.debug("Message [{}] handle end", envelope.getDeliveryTag());
                    },
                    // cancelCallback : callback when the consumer is cancelled.
                    defaultCancelCallback,
                    // shutdownSignalCallback : callback when the channel/connection is shut down.
                    defaultShutdownSignalCallback);
            } catch (IOException e) {
                LOG.error("Function basicConsume() with SelfAckConsumer throw IOException message -> {}",
                    e.getMessage(), e);
                throw new ReceiverStartException(e.getMessage(), e);
            }
            // # Set SelfAckConsumer end *****
        }
        // 如果[consumer]不为null, 设置consumer
        if (processor != null) {
            // # Set Consume start *****
            try {
                channel.basicConsume(
                    /*
                     * queue : the name of the queue
                     */
                    queueName,
                    /*
                     * autoAck : true if the server should consider messages acknowledged once delivered;
                     * false if the server should expect explicit acknowledgements
                     */
                    autoAck,
                    /*
                     * consumerTag : consumerTag a client-generated consumer tag to establish context
                     */
                    tag,
                    /*
                     * noLocal : true if the server should not deliver to this consumer messages published
                     * on this channel's connection.
                     * Note! that the RabbitMQ server does not support this flag.
                     */
                    false,
                    // exclusive : true if this is an exclusive consumer.
                    exclusive,
                    // arguments : a set of arguments for to consume.
                    args,
                    // deliverCallback : callback when a message is delivered.
                    (consumerTag, delivery) -> {
                        // 消息处理开始
                        Envelope envelope = delivery.getEnvelope();
                        byte[] body = delivery.getBody();
                        try {
                            LOG.debug("Message [{}] handle start", envelope.getDeliveryTag());
                            LOG.debug(
                                "Callback handleDelivery, consumerTag==[{}], deliveryTag==[{}], body.length==[{}]",
                                consumerTag, envelope.getDeliveryTag(), body.length);
                            T t;
                            try {
                                t = deserializer.deserialization(body);
                            } catch (Exception e) {
                                throw new DecodeException(e);
                            }
                            //TODO 加入消息处理器的异常处理
                            processor.process(t);
                        } catch (Exception e) {
                            LOG.error("Consumer accept msg==[{}] throw Exception -> {}",
                                StringSupport.toString(body), e.getMessage(), e);
                            dumpUnprocessableMsg(e, consumerTag, envelope, delivery.getProperties(), body);
                        }
                        if (!autoAck) {
                            if (ack(envelope.getDeliveryTag())) {
                                LOG.debug("Message handle and ack finished");
                            } else {
                                LOG.info("Ack failure envelope.getDeliveryTag()==[{}], Reject message", envelope.getDeliveryTag());
                                channel.basicReject(envelope.getDeliveryTag(), true);
                            }
                        }
                        LOG.debug("Message [{}] handle end", envelope.getDeliveryTag());
                        // 消息处理结束
                    },
                    // cancelCallback : callback when the consumer is cancelled.
                    defaultCancelCallback,
                    // shutdownSignalCallback : callback when the channel/connection is shut down.
                    defaultShutdownSignalCallback);
            } catch (IOException e) {
                LOG.error("Function basicConsume() with Consumer throw IOException message -> {}", e.getMessage(), e);
                throw new ReceiverStartException(e.getMessage(), e);
            }
            // # Set Consume end *****
        }
        newStartTime();
    }

    @Override
    public boolean reconnectSupported() {
        return true;
    }

    private final CancelCallback defaultCancelCallback = consumerTag ->
        LOG.info("CancelCallback receive consumerTag==[{}]", consumerTag);


    private final ConsumerShutdownSignalCallback defaultShutdownSignalCallback = (consumerTag, sig) -> {
        LOG.info("Consumer received ShutdownSignalException, consumerTag==[{}]", consumerTag);
        handleShutdownSignal(sig);
    };


    /**
     * @param cause       Throwable
     * @param consumerTag String
     * @param envelope    Envelope
     * @param properties  BasicProperties
     * @param body        byte[]
     * @throws IOException ioe
     */
    private void dumpUnprocessableMsg(Throwable cause, String consumerTag, Envelope envelope,
                                      BasicProperties properties, byte[] body) throws IOException {
        if (hasErrMsgExchange) {
            // Sent message to error dump exchange.
            LOG.error("Exception handling -> Sent to ErrMsgExchange [{}]", errMsgExchangeName);
            channel.basicPublish(errMsgExchangeName, errMsgRoutingKey, properties, body);
            LOG.error("Exception handling -> Sent to ErrMsgExchange [{}] finished", errMsgExchangeName);
        } else if (hasErrMsgQueue) {
            // Sent message to error dump queue.
            LOG.error("Exception handling -> Sent to ErrMsgQueue [{}]", errMsgQueueName);
            channel.basicPublish("", errMsgQueueName, properties, body);
            LOG.error("Exception handling -> Sent to ErrMsgQueue finished");
        } else {
            // Reject message and close connection.
            LOG.error("Exception handling -> Reject Msg [{}]", StringSupport.toString(body));
            channel.basicReject(envelope.getDeliveryTag(), true);
            LOG.error("Exception handling -> Reject Msg finished");
            closeIgnoreException();
            LOG.error("RabbitMqReceiver: [{}] already closed", receiverName);
            throw new MsgHandleException(
                "The message could not handle, and could not delivered to the error dump address."
                + "\n The connection was closed.",
                cause);
        }
    }

    /**
     * @param deliveryTag long
     * @return boolean
     */
    private boolean ack(long deliveryTag) {
        return ack0(deliveryTag, 0);
    }

    /**
     * @param deliveryTag long
     * @param retry       int
     * @return boolean
     */
    private boolean ack0(long deliveryTag, int retry) {
        if (retry == maxAckTotal) {
            LOG.error("Has been retry ack {}, Quit ack", maxAckTotal);
            return false;
        }
        LOG.debug("Has been retry ack {}, Do next ack", retry);
        try {
            int reconnectionCount = 0;
            while (!isConnected()) {
                reconnectionCount++;
                LOG.debug("Detect connection isConnected() == false, Reconnection count {}", reconnectionCount);
                closeAndReconnection();
                if (reconnectionCount > maxAckReconnection) {
                    LOG.debug("Reconnection count -> {}, Quit current ack", reconnectionCount);
                    break;
                }
            }
            if (isConnected()) {
                LOG.debug("Last detect connection isConnected() == true, Reconnection count {}", reconnectionCount);
                channel.basicAck(deliveryTag, multipleAck);
                LOG.debug("Call function channel.basicAck() finished");
                return true;
            } else {
                LOG.error("Last detect connection isConnected() == false, Reconnection count {}", reconnectionCount);
                LOG.error("Can't call function channel.basicAck()");
                return ack0(deliveryTag, retry);
            }
        } catch (IOException e) {
            LOG.error("Call function channel.basicAck(deliveryTag==[{}], multiple==[{}]) throw IOException -> {}",
                deliveryTag, multipleAck, e.getMessage(), e);
            return ack0(deliveryTag, ++retry);
        }
    }

    @Override
    public boolean closeIgnoreException() {
        LOG.info("Call function destroy() from Receiver name==[{}]", receiverName);
        return super.closeIgnoreException();
    }

    @Override
    public String getName() {
        return receiverName;
    }

    @Override
    public void reconnect() throws ConnectionBreakException, ReceiverStartException {
        closeAndReconnection();
        receive();
    }

    /**
     * @author yellow013
     */
    public static class AckDelegate {

        private final AdvancedRmqConsumer<?> receiver;

        private AckDelegate(AdvancedRmqConsumer<?> receiver) {
            this.receiver = receiver;
        }

        public boolean ack(long deliveryTag) {
            return receiver.ack(deliveryTag);
        }

    }

    @FunctionalInterface
    public interface SelfAckConsumer<T> {

        void handleMessage(final AckDelegate ackDelegate, final String consumerTag, Message<T> msg);

    }

    public record Message<T>(
        Envelope envelope,
        BasicProperties properties,
        T body
    ) {
    }

}
