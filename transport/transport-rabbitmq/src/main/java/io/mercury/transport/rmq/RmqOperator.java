package io.mercury.transport.rmq;

import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import io.mercury.common.datetime.DateTimeUtil;
import io.mercury.common.lang.Validator;
import io.mercury.transport.rmq.config.RmqConnection;
import io.mercury.transport.rmq.declare.AmqpExchange;
import io.mercury.transport.rmq.declare.AmqpQueue;
import io.mercury.transport.rmq.exception.DeclareException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

public final class RmqOperator extends RmqTransport {

    /**
     * Create OperationalChannel of host, port, username and password
     *
     * @param host     String
     * @param port     int
     * @param username String
     * @param password String
     * @return RmqOperator
     */
    public static RmqOperator with(String host, int port, String username, String password) {
        return with(RmqConnection.with(host, port, username, password).build());
    }

    /**
     * Create OperationalChannel of host, port, username, password and virtualHost
     *
     * @param host        String
     * @param port        int
     * @param username    String
     * @param password    String
     * @param virtualHost String
     * @return RmqOperator
     */
    public static RmqOperator with(String host, int port, String username, String password, String virtualHost) {
        return with(RmqConnection.with(host, port, username, password, virtualHost).build());
    }

    /**
     * Create OperationalChannel of RmqConnection
     *
     * @param connection RmqConnection
     * @return RmqOperator
     */
    public static RmqOperator with(RmqConnection connection) {
        return new RmqOperator(connection);
    }

    /**
     * Create OperationalChannel of Channel
     *
     * @param channel Channel
     * @return RmqOperator
     */
    public static RmqOperator with(Channel channel) {
        return new RmqOperator(channel);
    }

    private RmqOperator(RmqConnection connection) {
        super("declarator-" + DateTimeUtil.datetimeOfMillisecond(), connection);
        createConnection();
    }

    private RmqOperator(Channel channel) {
        super("declarator-with-channel-" + channel.getChannelNumber());
        this.channel = channel;
    }

    /**
     * @param queue String queue name
     * @throws DeclareException de
     */
    public boolean declareQueue(@Nonnull String queue) throws DeclareException {
        return declareQueue(queue, true, false, false, null);
    }

    /**
     * @param queue AmqpQueue
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareQueue(@Nonnull AmqpQueue queue) throws DeclareException {
        try {
            Validator.nonNull(queue, "queue");
        } catch (Exception e) {
            throw DeclareException.becauseOf(e);
        }
        return declareQueue(queue.getName(), queue.isDurable(), queue.isExclusive(), queue.isAutoDelete(),
                queue.getArgs());
    }

    /**
     * @param queue      String
     * @param durable    boolean
     * @param exclusive  boolean
     * @param autoDelete boolean
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareQueue(@Nonnull String queue, boolean durable, boolean exclusive, boolean autoDelete,
                                Map<String, Object> args) throws DeclareException {
        try {
            Validator.nonEmpty(queue, "queue");
        } catch (Exception e) {
            throw DeclareException.becauseOf(e);
        }
        try {
            channel.queueDeclare(queue, durable, exclusive, autoDelete, args);
            return true;
        } catch (Exception e) {
            throw DeclareException.declareQueueError(queue, durable, exclusive, autoDelete, args, e);
        }
    }

    /**
     * @param exchange AmqpExchange
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareExchange(@Nonnull AmqpExchange exchange) throws DeclareException {
        try {
            Validator.nonNull(exchange, "exchange");
        } catch (Exception e) {
            throw DeclareException.becauseOf(e);
        }
        return switch (exchange.getType()) {
            case Direct -> declareDirectExchange(exchange.getName(), exchange.isDurable(), exchange.isAutoDelete(),
                    exchange.isInternal(), exchange.getArgs());
            case Fanout -> declareFanoutExchange(exchange.getName(), exchange.isDurable(), exchange.isAutoDelete(),
                    exchange.isInternal(), exchange.getArgs());
            case Topic -> declareTopicExchange(exchange.getName(), exchange.isDurable(), exchange.isAutoDelete(),
                    exchange.isInternal(), exchange.getArgs());
            default -> false;
        };
    }

    /**
     * @param exchange String
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareDirectExchange(@Nonnull String exchange) throws DeclareException {
        return declareDirectExchange(exchange, true, false, false, null);
    }

    /**
     * @param exchange   String
     * @param durable    boolean
     * @param autoDelete boolean
     * @param internal   boolean
     * @param args       Map<String, Object>
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareDirectExchange(@Nonnull String exchange, boolean durable, boolean autoDelete,
                                         boolean internal, Map<String, Object> args) throws DeclareException {
        return declareExchange(exchange, BuiltinExchangeType.DIRECT, durable, autoDelete, internal, args);
    }

    /**
     * @param exchange String
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareFanoutExchange(@Nonnull String exchange) throws DeclareException {
        return declareFanoutExchange(exchange, true, false, false, null);
    }

    /**
     * @param exchange   String
     * @param durable    boolean
     * @param autoDelete boolean
     * @param internal   boolean
     * @param args       Map<String, Object>
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareFanoutExchange(@Nonnull String exchange, boolean durable, boolean autoDelete,
                                         boolean internal, Map<String, Object> args) throws DeclareException {
        return declareExchange(exchange, BuiltinExchangeType.FANOUT, durable, autoDelete, internal, args);
    }

    /**
     * @param exchange String
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareTopicExchange(@Nonnull String exchange) throws DeclareException {
        return declareTopicExchange(exchange, true, false, false, null);
    }

    /**
     * @param exchange   String
     * @param durable    boolean
     * @param autoDelete boolean
     * @param internal   boolean
     * @param args       Map<String, Object>
     * @return boolean
     * @throws DeclareException de
     */
    public boolean declareTopicExchange(@Nonnull String exchange, boolean durable, boolean autoDelete,
                                        boolean internal, Map<String, Object> args) throws DeclareException {
        return declareExchange(exchange, BuiltinExchangeType.TOPIC, durable, autoDelete, internal, args);
    }

    private boolean declareExchange(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
                                    boolean internal, Map<String, Object> arg) throws DeclareException {
        try {
            Validator.nonEmpty(exchange, "exchange");
        } catch (Exception e) {
            throw DeclareException.becauseOf(e);
        }
        try {
            /*
             * exchange : the name of the exchange.
             * type : the exchange type.
             * durable : true if we are declaring a durable exchange (the exchange will
             * survive a server restart)
             * autoDelete : true if the server should delete the exchange when it is no
             * longer in use.
             * internal : true if the exchange is internal, i.e. can't be directly published
             * to by a client.
             * arguments : other properties (construction arguments) for the exchange.
             */
            channel.exchangeDeclare(exchange, type, durable, autoDelete, internal, arg);
            return true;
        } catch (IOException e) {
            throw DeclareException.declareExchangeError(exchange, type, durable, autoDelete, internal, arg, e);
        }
    }

    /**
     * @param queue    String
     * @param exchange String
     * @return boolean
     * @throws DeclareException de
     */
    public boolean bindQueue(String queue, String exchange) throws DeclareException {
        return bindQueue(queue, exchange, "");
    }

    /**
     * @param queue      String
     * @param exchange   String
     * @param routingKey String
     * @return boolean
     * @throws DeclareException de
     */
    public boolean bindQueue(String queue, String exchange, String routingKey) throws DeclareException {
        try {
            Validator.nonEmpty(queue, "queue");
            Validator.nonEmpty(exchange, "exchange");
        } catch (Exception e) {
            throw DeclareException.becauseOf(e);
        }
        try {
            /*
             * queue : the name of the queue.
             * exchange : the name of the exchange.
             * routingKey : the routing key to use for the binding.
             */
            channel.queueBind(queue, exchange, routingKey == null ? "" : routingKey);
            return true;
        } catch (IOException e) {
            throw DeclareException.bindQueueError(queue, exchange, routingKey, e);
        }
    }

    /**
     * @param destExchange   String
     * @param sourceExchange String
     * @return boolean
     * @throws DeclareException de
     */
    public boolean bindExchange(String destExchange, String sourceExchange) throws DeclareException {
        return bindExchange(destExchange, sourceExchange, "");
    }

    /**
     * @param destExchange   String
     * @param sourceExchange String
     * @param routingKey     String
     * @return boolean
     * @throws DeclareException de
     */
    public boolean bindExchange(String destExchange, String sourceExchange, String routingKey)
            throws DeclareException {
        try {
            Validator.nonEmpty(destExchange, "destExchange");
            Validator.nonEmpty(sourceExchange, "sourceExchange");
        } catch (Exception e) {
            throw DeclareException.becauseOf(e);
        }
        try {
            /*
             * destination : the name of the exchange to which messages flow across the
             * binding.
             * source : the name of the exchange from which messages flow across the
             * binding.
             * routingKey : the routing key to use for the binding.
             */
            channel.exchangeBind(destExchange, sourceExchange, routingKey == null ? "" : routingKey);
            return true;
        } catch (IOException e) {
            throw DeclareException.bindExchangeError(destExchange, sourceExchange, routingKey, e);
        }
    }

    /**
     * @param queue String - the name of the queue
     * @param force boolean
     * @return int
     * @throws IOException ioe
     */
    public int deleteQueue(String queue, boolean force) throws IOException {
        /*
         * queue : the name of the queue.
         * ifUnused : true if the queue should be deleted only if not in use.
         * ifEmpty : true if the queue should be deleted only if empty.
         */
        DeleteOk delete = channel.queueDelete(queue, !force, !force);
        return delete.getMessageCount();
    }

    /**
     * @param exchange String
     * @param force    boolean
     * @return boolean
     * @throws IOException ioe
     */
    public boolean deleteExchange(String exchange, boolean force) throws IOException {
        /*
         * exchange : the name of the exchange.
         * ifUnused : true to indicate that the exchange is only to be deleted if it is
         * unused.
         */
        channel.exchangeDelete(exchange, !force);
        return true;
    }

    public static void main(String[] args) {
        try {
            RmqOperator declarator = with("127.0.0.1", 5672, "guest", "guest");
            System.out.println(declarator.isConnected());
            try {
                declarator.declareFanoutExchange("MarketData", true, false, false, null);
            } catch (DeclareException e) {
                e.printStackTrace();
            }
            declarator.close();
            System.out.println(declarator.isConnected());
        } catch (Exception ignored) {

        }
    }

}
