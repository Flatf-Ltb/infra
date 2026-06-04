package io.flatf.infra.transport.rmq;

import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import io.flatf.common.character.Charsets;
import io.flatf.common.collections.MutableLists;
import io.flatf.common.concurrent.queue.MultiConsumerQueue;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.serialization.specific.BytesDeserializer;
import io.flatf.common.serialization.specific.BytesSerializer;
import io.flatf.infra.serialization.json.JsonWriter;
import io.flatf.infra.transport.rmq.config.RmqConnectionConfig;
import io.flatf.infra.transport.rmq.declare.AmqpExchange;
import io.flatf.infra.transport.rmq.declare.QueueRelationship;
import io.flatf.infra.transport.rmq.exception.DeclareException;
import lombok.Getter;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class RmqBuffer<E> implements MultiConsumerQueue<E>, Closeable {

    private static final Logger LOG = Log4j2LoggerFactory.getLogger(RmqBuffer.class);

    @Getter
    private final RmqConnectionConfig connection;

    private final RmqChannel channel;
    private final String queueName;
    private final List<String> exchangeNames;
    private final List<String> routingKeys;

    private final BytesSerializer<E> serializer;
    private final BytesDeserializer<E> deserializer;

    private final String name;

    /**
     * @param <E>          E
     * @param connection   RmqConnection
     * @param queueName    String
     * @param serializer   BytesSerializer<E>
     * @param deserializer BytesDeserializer<E>
     * @return RmqBuffer<E>
     * @throws DeclareException de
     */
    public static <E> RmqBuffer<E> newQueue(RmqConnectionConfig connection, String queueName,
                                            BytesSerializer<E> serializer, BytesDeserializer<E> deserializer)
        throws DeclareException {
        return new RmqBuffer<>(connection, queueName, MutableLists.newFastList(), MutableLists.newFastList(),
            serializer, deserializer);
    }

    /**
     * @param <E>           E
     * @param connection    RmqConnection
     * @param queueName     String
     * @param exchangeNames List<String>
     * @param routingKeys   List<String>
     * @param serializer    BytesSerializer<E>
     * @param deserializer  BytesDeserializer<E>
     * @return RmqBuffer<E>
     * @throws DeclareException de
     */
    public static <E> RmqBuffer<E> newQueue(RmqConnectionConfig connection, String queueName,
                                            List<String> exchangeNames, List<String> routingKeys,
                                            BytesSerializer<E> serializer, BytesDeserializer<E> deserializer)
        throws DeclareException {
        return new RmqBuffer<>(connection, queueName, exchangeNames, routingKeys, serializer, deserializer);
    }

    /**
     * @param connection    RmqConnection
     * @param queueName     String
     * @param exchangeNames List<String>
     * @param routingKeys   List<String>
     * @param serializer    BytesSerializer<E>
     * @param deserializer  BytesDeserializer<E>
     * @throws DeclareException de
     */
    private RmqBuffer(RmqConnectionConfig connection, String queueName,
                      List<String> exchangeNames, List<String> routingKeys,
                      BytesSerializer<E> serializer, BytesDeserializer<E> deserializer)
        throws DeclareException {
        this.connection = connection;
        this.queueName = queueName;
        this.exchangeNames = exchangeNames;
        this.routingKeys = routingKeys;
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.channel = RmqChannel.with(connection);
        this.name = "rabbitmq-buffer::" + connection.connectionInfo() + "/" + queueName;
        declareQueue();
    }

    /**
     * @throws DeclareException de
     */
    private void declareQueue() throws DeclareException {
        QueueRelationship relationship = QueueRelationship.named(queueName).binding(
            // 如果routingKeys为空集合, 则创建fanout交换器, 否则创建直接交换器
            exchangeNames.stream()
                .map(exchangeName -> routingKeys.isEmpty()
                    ? AmqpExchange.fanout(exchangeName)
                    : AmqpExchange.direct(exchangeName))
                .toList(),
            routingKeys);
        relationship.declare(RmqOperator.with(channel.internalChannel()));
    }

    @Override
    public boolean enqueue(E e) {
        byte[] msg = serializer.serialize(e);
        try {
            channel.internalChannel().basicPublish("", queueName, null, msg);
            return true;
        } catch (IOException ioe) {
            LOG.error("enqueue basicPublish throw -> {}", ioe.getMessage(), ioe);
            return false;
        }
    }

    @Override
    public E dequeue() {
        GetResponse response = basicGet();
        if (response == null)
            return null;
        byte[] body = response.getBody();
        if (body == null)
            return null;
        basicAck(response.getEnvelope());
        return deserializer.deserialization(body);
    }

    @Override
    public boolean pollAndApply(@Nonnull PollFunction<E> function) {
        GetResponse response = basicGet();
        if (response == null)
            return false;
        byte[] body = response.getBody();
        if (body == null)
            return false;
        if (!function.apply(deserializer.deserialization(body))) {
            LOG.error("PollFunction failure, no ack");
            return false;
        }
        return basicAck(response.getEnvelope());
    }

    /**
     * @return GetResponse
     */
    private GetResponse basicGet() {
        try {
            return channel.internalChannel().basicGet(queueName, false);
        } catch (IOException ioe) {
            LOG.error("poll basicGet throw -> {}", ioe.getMessage(), ioe);
            return null;
        }
    }

    /**
     * @param envelope Envelope
     * @return boolean
     */
    private boolean basicAck(Envelope envelope) {
        try {
            channel.internalChannel().basicAck(envelope.getDeliveryTag(), false);
            return true;
        } catch (IOException ioe) {
            LOG.error("poll basicAck throw -> {}", ioe.getMessage(), ioe);
            return false;
        }
    }

    @Override
    public String getQueueName() {
        return name;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public QueueType getQueueType() {
        return QueueType.MPMC;
    }


    public static void main(String[] args) {

        RmqConnectionConfig connection = RmqConnectionConfig.with("127.0.0.1", 5672, "user", "password").build();

        try (RmqBuffer<String> testQueue = newQueue(
            connection, "rmq_test",
            str -> JsonWriter.toJson(str).getBytes(Charsets.UTF8),
            (bytes, reuse) -> new String(bytes, Charsets.UTF8))) {

            testQueue.pollAndApply(str -> {
                System.out.println(str);
                return true;
            });
        } catch (DeclareException | IOException e) {
            e.printStackTrace();
        }

    }

}
