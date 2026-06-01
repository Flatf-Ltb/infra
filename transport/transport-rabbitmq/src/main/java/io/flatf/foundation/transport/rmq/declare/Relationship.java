package io.flatf.foundation.transport.rmq.declare;

import io.flatf.foundation.common.annotation.AbstractFunction;
import io.flatf.foundation.common.collections.MutableLists;
import io.flatf.foundation.common.lang.Validator;
import io.flatf.foundation.common.log4j2.Log4j2LoggerFactory;
import io.flatf.foundation.transport.rmq.RmqOperator;
import io.flatf.foundation.transport.rmq.exception.DeclareException;
import org.eclipse.collections.api.list.MutableList;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

public abstract class Relationship {

    protected static final Logger log = Log4j2LoggerFactory.getLogger(Relationship.class);

    protected final MutableList<Binding> bindings = MutableLists.newFastList();

    /**
     * @param operator RmqOperator
     * @throws DeclareException e
     */
    public void declare(@Nonnull RmqOperator operator) throws DeclareException {
        Validator.nonNull(operator, "operator");
        declare0(operator);
        for (Binding binding : bindings)
            declareBinding(operator, binding);
    }

    /**
     * @param operator RmqOperator
     * @param binding  Binding
     * @throws DeclareException e
     */
    private void declareBinding(RmqOperator operator, Binding binding) throws DeclareException {
        var source = binding.source;
        try {
            operator.declareExchange(source);
        } catch (DeclareException declareException) {
            log.error("Declare source exchange failure -> {}", source);
            throw declareException;
        }
        String routingKey = binding.routingKey;
        switch (binding.destType) {
            case Exchange -> {
                var destExchange = binding.destExchange;
                try {
                    operator.declareExchange(destExchange);
                } catch (DeclareException e) {
                    log.error("Declare dest exchange failure -> destExchange==[{}]", destExchange, e);
                    throw e;
                }
                try {
                    operator.bindExchange(destExchange.name(), source.name(), routingKey);
                } catch (DeclareException e) {
                    log.error("Declare bind exchange failure -> destExchange==[{}], source==[{}], routingKey==[{}]",
                        destExchange, source, routingKey, e);
                    throw e;
                }
            }
            case Queue -> {
                var destQueue = binding.destQueue;
                try {
                    operator.declareQueue(destQueue);
                } catch (DeclareException e) {
                    log.error("Declare dest queue failure -> destQueue==[{}]", destQueue, e);
                    throw e;
                }
                try {
                    operator.bindQueue(destQueue.name(), source.name(), routingKey);
                } catch (DeclareException e) {
                    log.error("Declare bind queue failure -> destQueue==[{}], source==[{}], routingKey==[{}]", destQueue,
                        source, routingKey, e);
                    throw e;
                }
            }
            default -> log.error("Unknown destType -> {}", binding.destType);
        }
    }

    @AbstractFunction
    protected abstract void declare0(RmqOperator operator);

    public static final class Binding {

        private final AmqpExchange source;
        private final AmqpExchange destExchange;
        private final AmqpQueue destQueue;
        private final String routingKey;
        private final DestType destType;

        /**
         * @param source AmqpExchange
         * @param dest   AmqpExchange
         */
        Binding(AmqpExchange source, AmqpExchange dest) {
            this(source, dest, "");
        }

        /**
         * @param source     AmqpExchange
         * @param dest       AmqpExchange
         * @param routingKey String
         */
        Binding(AmqpExchange source, AmqpExchange dest, String routingKey) {
            this(source, dest, null, routingKey, DestType.Exchange);
        }

        /**
         * @param source AmqpExchange
         * @param dest   AmqpQueue
         */
        Binding(AmqpExchange source, AmqpQueue dest) {
            this(source, dest, "");
        }

        /**
         * @param source     AmqpExchange
         * @param dest       AmqpQueue
         * @param routingKey String
         */
        Binding(AmqpExchange source, AmqpQueue dest, String routingKey) {
            this(source, null, dest, routingKey, DestType.Queue);
        }

        /**
         * @param source       AmqpExchange
         * @param destExchange AmqpExchange
         * @param destQueue    AmqpQueue
         * @param routingKey   String
         * @param destType     DestType
         */
        private Binding(AmqpExchange source, AmqpExchange destExchange,
                        AmqpQueue destQueue, String routingKey,
                        DestType destType) {
            this.source = source;
            this.destExchange = destExchange;
            this.destQueue = destQueue;
            this.routingKey = routingKey;
            this.destType = destType;
        }

    }

    enum DestType {
        Exchange,
        Queue
    }

}
