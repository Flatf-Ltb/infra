package io.flatf.transport.rmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.flatf.common.codec.DecodeException;
import io.flatf.common.functional.Processor;
import io.flatf.common.lang.Validator;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.util.StringSupport;
import io.flatf.transport.api.Receiver;
import io.flatf.transport.exception.ConnectionBreakException;
import io.flatf.transport.exception.ConnectionFailedException;
import io.flatf.transport.exception.ReceiverStartException;
import io.flatf.transport.rmq.config.RmqConnectionConfig;
import io.flatf.transport.rmq.config.RmqConsumerConfig;
import io.flatf.transport.rmq.declare.ExchangeRelationship;
import io.flatf.transport.rmq.declare.QueueRelationship;
import io.flatf.transport.rmq.exception.DeclareException;
import io.flatf.transport.rmq.exception.DeclareRuntimeException;
import io.flatf.transport.rmq.exception.MsgHandleException;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.Function;

import static io.flatf.common.character.Charsets.UTF8;
import static io.flatf.common.datetime.DateTimeUtil.datetimeOfMillisecond;
import static io.flatf.common.util.StringSupport.nonEmpty;

/**
 * @author yellow013<br>
 * <p>
 * [已完成]改造升级, 使用共同的构建者建立Exchange, RoutingKey, Queue的绑定关系
 */
public class RmqConsumer<T> extends RmqTransport implements Receiver, Runnable {

    private static final Logger log = Log4j2LoggerFactory.getLogger(RmqConsumer.class);

    // 接收消息使用的反序列化器
    private final Function<byte[], T> deserializer;

    // 接收消息时使用的回调函数
    private final Processor<T> processor;

    // 接受者QueueDeclare
    private final QueueRelationship usedQueue;

    // 接受者QueueName
    private final String queueName;

    // 消息无法处理时发送到的错误消息ExchangeDeclare
    private final ExchangeRelationship errMsgExchange;

    // 消息无法处理时发送到的错误消息Exchange使用的RoutingKey
    private final String errMsgRoutingKey;

    // 消息无法处理时发送到的错误消息QueueDeclare
    private final QueueRelationship errMsgQueue;

    // 消息无法处理时发送到的错误消息Exchange
    private String errMsgExchangeName;

    // 消息无法处理时发送到的错误消息Queue
    private String errMsgQueueName;

    // 是否有错误消息Exchange
    private boolean hasErrMsgExchange;

    // 是否有错误消息Queue
    private boolean hasErrMsgQueue;

    // 自动ACK
    private final boolean autoAck;

    // 一次ACK多条
    private final boolean multipleAck;

    // ACK最大自动重试次数
    private final int maxAckTotal;

    // ACK最大自动重连次数
    private final int maxAckReconnection;

    // QOS预取
    private final int qos;

    // Receiver名称
    private final String receiverName;

    /**
     * @param config    RmqReceiverConfig
     * @param processor Consumer<byte[]>
     * @return RmqConsumer<byte[]>
     */
    public static RmqConsumer<byte[]> create(@Nonnull RmqConsumerConfig config,
                                             @Nonnull Processor<byte[]> processor) {
        return create(null, config, processor);
    }

    /**
     * @param tag       String
     * @param config    RmqReceiverConfig
     * @param processor Consumer<byte[]>
     * @return RmqConsumer<byte[]>
     */
    public static RmqConsumer<byte[]> create(@Nullable String tag,
                                             @Nonnull RmqConsumerConfig config,
                                             @Nonnull Processor<byte[]> processor) {
        return create(tag, config, msg -> msg, processor);
    }

    /**
     * @param <T>          T
     * @param config       RmqReceiverConfig
     * @param deserializer Function<byte[], T>
     * @param processor    Processor<T>
     * @return RmqConsumer<T>
     */
    public static <T> RmqConsumer<T> create(@Nonnull RmqConsumerConfig config,
                                            @Nonnull Function<byte[], T> deserializer,
                                            @Nonnull Processor<T> processor) {
        return create(null, config, deserializer, processor);
    }

    /**
     * @param <T>          T
     * @param tag          String
     * @param config       RmqReceiverConfig
     * @param deserializer Function<byte[], T>
     * @param processor    Processor<T>
     * @return RmqConsumer<T>
     */
    public static <T> RmqConsumer<T> create(@Nullable String tag,
                                            @Nonnull RmqConsumerConfig config,
                                            @Nonnull Function<byte[], T> deserializer,
                                            @Nonnull Processor<T> processor) {
        return new RmqConsumer<>(tag, config, deserializer, processor);
    }

    /**
     * @param tag          String
     * @param cfg          RmqReceiverConfig
     * @param deserializer Function<byte[], T>
     * @param processor     Consumer<T>
     */
    private RmqConsumer(@Nullable String tag, @Nonnull RmqConsumerConfig cfg,
                        @Nonnull Function<byte[], T> deserializer,
                        @Nonnull Processor<T> processor)
        throws ConnectionFailedException {
        super(nonEmpty(tag) ? tag : "recv-" + datetimeOfMillisecond(), cfg.getConnectionConfig());
        this.usedQueue = cfg.queue();
        this.queueName = usedQueue.queueName();
        this.deserializer = deserializer;
        this.processor = processor;
        this.errMsgExchange = cfg.errMsgExchange();
        this.errMsgRoutingKey = cfg.errMsgRoutingKey();
        this.errMsgQueue = cfg.errMsgQueue();
        this.autoAck = cfg.ackOptions().autoAck();
        this.multipleAck = cfg.ackOptions().multipleAck();
        this.maxAckTotal = cfg.ackOptions().maxAckTotal();
        this.maxAckReconnection = cfg.ackOptions().maxAckReconnection();
        this.qos = cfg.ackOptions().qos();
        this.receiverName = "recv::" + connectionConfig.connectionInfo() + "$" + queueName;
        createConnection();
        declare();
    }

    private void declare() {
        RmqOperator operator = RmqOperator.with(channel);
        try {
            this.usedQueue.declare(operator);
        } catch (DeclareException e) {
            log.error("Queue declare throw exception -> connection configurator info : {}, error message : {}",
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
            log.error(
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
            log.error("ErrorMsgQueue declare throw exception -> connection configurator info : {}, error message : {}",
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
        Validator.nonNull(deserializer, "deserializer");
        Validator.nonNull(processor, "processor");
        // # Set QOS parameter start *****
        if (!autoAck) {
            try {
                channel.basicQos(qos);
            } catch (IOException e) {
                log.error("Function basicQos() qos==[{}] throw IOException message -> {}", qos, e.getMessage(), e);
                throw new ReceiverStartException(e.getMessage(), e);
            }
        }
        // # Set QOS parameter end *****
        // # Set Consume start *****
        try {
            channel.basicConsume(
                // param1: the name of the queue
                queueName,
                // param2: true if the server should consider messages acknowledged once
                // delivered; false if the server should expect explicit acknowledgement
                autoAck,
                // param3: a client-generated consumer tag to establish context
                tag,
                // param4: an interface to the consumer object
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                                               byte[] body) throws IOException {
                        try {
                            log.debug("Message handle start");
                            log.debug("Callback handleDelivery, consumerTag==[{}], deliveryTag==[{}] body.length==[{}]",
                                consumerTag, envelope.getDeliveryTag(), body.length);
                            T apply;
                            try {
                                apply = deserializer.apply(body);
                            } catch (Exception e) {
                                throw new DecodeException(e);
                            }
                            processor.process(apply);
                            log.debug("Callback handleDelivery() end");
                        } catch (Exception e) {
                            log.error("Process msg==[{}] throw Exception -> {}",
                                StringSupport.toString(body), e.getMessage(), e);
                            dumpUnprocessableMsg(e, consumerTag, envelope, properties, body);
                        }
                        if (!autoAck) {
                            if (ack(envelope.getDeliveryTag())) {
                                log.debug("Message handle and ack finished");
                            } else {
                                log.info("Ack failure envelope.getDeliveryTag()==[{}], Reject message", envelope.getDeliveryTag());
                                channel.basicReject(envelope.getDeliveryTag(), true);
                            }
                        }
                    }
                });
        } catch (IOException e) {
            log.error("Func -> RabbitMqReceiver::basicConsume() IOException msg -> {}", e.getMessage(), e);
            throw new ReceiverStartException(e.getMessage(), e);
        }
        // # Set Consume end *****
    }


    /**
     * @param cause       Throwable
     * @param consumerTag String
     * @param envelope    Envelope
     * @param properties  BasicProperties
     * @param body        byte[]
     * @throws IOException ioe
     */
    private void dumpUnprocessableMsg(Throwable cause, String consumerTag,
                                      Envelope envelope, BasicProperties properties,
                                      byte[] body) throws IOException {
        if (hasErrMsgExchange) {
            // Sent message to error dump exchange.
            log.error("Exception handling -> Sent to ErrMsgExchange [{}]", errMsgExchangeName);
            channel.basicPublish(errMsgExchangeName, errMsgRoutingKey, properties, body);
            log.error("Exception handling -> Sent to ErrMsgExchange [{}] finished", errMsgExchangeName);
        } else if (hasErrMsgQueue) {
            // Sent message to error dump queue.
            log.error("Exception handling -> Sent to ErrMsgQueue [{}]", errMsgQueueName);
            channel.basicPublish("", errMsgQueueName, properties, body);
            log.error("Exception handling -> Sent to ErrMsgQueue finished");
        } else {
            // Reject message and close connection.
            log.error("Exception handling -> Reject Msg [{}]", StringSupport.toString(body));
            channel.basicReject(envelope.getDeliveryTag(), true);
            log.error("Exception handling -> Reject Msg finished");
            closeIgnoreException();
            log.error("RabbitMqReceiver: [{}] already closed", receiverName);
            throw new MsgHandleException(
                "The message could not handle, and could not delivered to the error dump address."
                + "\n The connection was closed.", cause);
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
            log.error("Has been retry ack {}, Quit ack", maxAckTotal);
            return false;
        }
        log.debug("Has been retry ack {}, Do next ack", retry);
        try {
            int reconnectionCount = 0;
            while (!isConnected()) {
                reconnectionCount++;
                log.debug("Detect connection isConnected() == false, Reconnection count {}", reconnectionCount);
                closeAndReconnection();
                if (reconnectionCount > maxAckReconnection) {
                    log.debug("Reconnection count -> {}, Quit current ack", reconnectionCount);
                    break;
                }
            }
            if (isConnected()) {
                log.debug("Last detect connection isConnected() == true, Reconnection count {}", reconnectionCount);
                channel.basicAck(deliveryTag, multipleAck);
                log.debug("Call function Channel::basicAck() finished");
                return true;
            } else {
                log.error("Last detect connection isConnected() == false, Reconnection count {}", reconnectionCount);
                log.error("Can't call function channel.basicAck()");
                return ack0(deliveryTag, retry);
            }
        } catch (IOException e) {
            log.error("Call func Channel::basicAck(deliveryTag==[{}], multiple==[{}]) throw IOException -> {}",
                deliveryTag, multipleAck, e.getMessage(), e);
            return ack0(deliveryTag, ++retry);
        }
    }

    @Override
    public boolean closeIgnoreException() {
        log.info("Call func -> RabbitMqReceiver::closeIgnoreException(), receiverName==[{}]", receiverName);
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

    public static void main(String[] args) {
        try (RmqConsumer<byte[]> receiver = RmqConsumer
            .create("test",
                RmqConsumerConfig.with(RmqConnectionConfig.with("127.0.0.1", 5672, "user", "u_pass").build(),
                    QueueRelationship.named("TEST")).build(),
                msg -> System.out.println(new String(msg, UTF8)))) {
            receiver.receive();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
