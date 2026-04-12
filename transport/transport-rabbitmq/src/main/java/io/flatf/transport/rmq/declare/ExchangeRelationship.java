package io.flatf.transport.rmq.declare;

import io.flatf.common.lang.Validator;
import io.flatf.serialization.json.JsonWriter;
import io.flatf.transport.rmq.RmqOperator;
import io.flatf.transport.rmq.exception.DeclareException;
import org.apache.commons.collections4.CollectionUtils;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;

import static io.flatf.common.collections.MutableLists.newFastList;

/**
 * 定义Exchange和其他实体绑定关系
 *
 * @author yellow013
 */
public final class ExchangeRelationship extends Relationship {

    public final static ExchangeRelationship Anonymous = new ExchangeRelationship(AmqpExchange.Anonymous);

    /**
     * exchange
     */
    private final AmqpExchange exchange;

    /**
     * @param exchange AmqpExchange
     */
    private ExchangeRelationship(@Nonnull AmqpExchange exchange) {
        this.exchange = exchange;
    }

    /**
     * @param exchangeName String
     * @return ExchangeRelationship
     */
    public static ExchangeRelationship fanout(@Nonnull String exchangeName) {
        return withExchange(AmqpExchange.fanout(exchangeName));
    }

    /**
     * @param exchangeName String
     * @return ExchangeRelationship
     */
    public static ExchangeRelationship direct(@Nonnull String exchangeName) {
        return withExchange(AmqpExchange.direct(exchangeName));
    }

    /**
     * @param exchangeName String
     * @return ExchangeRelationship
     */
    public static ExchangeRelationship topic(@Nonnull String exchangeName) {
        return withExchange(AmqpExchange.topic(exchangeName));
    }

    /**
     * @param exchange AmqpExchange
     * @return ExchangeRelationship
     */
    public static ExchangeRelationship withExchange(@Nonnull AmqpExchange exchange) {
        Validator.nonNull(exchange, "exchange");
        return new ExchangeRelationship(exchange);
    }

    @Override
    protected void declare0(RmqOperator operator) {
        try {
            operator.declareExchange(exchange);
        } catch (DeclareException e) {
            log.error("Declare Exchange failure -> {}", exchange);
            throw new RuntimeException(e);
        }
    }

    /**
     * @return the exchange name
     */
    public String getExchangeName() {
        return exchange.getName();
    }

    /**
     * @param durable boolean
     * @return ExchangeRelationship
     */
    public ExchangeRelationship setDurable(boolean durable) {
        exchange.setDurable(durable);
        return this;
    }

    /**
     * @param autoDelete boolean
     * @return ExchangeRelationship
     */
    public ExchangeRelationship setAutoDelete(boolean autoDelete) {
        exchange.setAutoDelete(autoDelete);
        return this;
    }

    /**
     * @param internal boolean
     * @return ExchangeRelationship
     */
    public ExchangeRelationship setInternal(boolean internal) {
        exchange.setInternal(internal);
        return this;
    }

    /**
     * @param args Map<String, Object>
     * @return ExchangeRelationship
     */
    public ExchangeRelationship setArgs(Map<String, Object> args) {
        exchange.setArgs(args);
        return this;
    }

    /**
     * @param destExchanges AmqpExchange...
     * @return ExchangeRelationship
     */
    public ExchangeRelationship bindingExchanges(AmqpExchange... destExchanges) {
        return bindingExchanges(newFastList(destExchanges), null);
    }

    /**
     * @param destExchanges Collection<AmqpExchange>
     * @param routingKeys   Collection<String>
     * @return ExchangeRelationship
     */
    public ExchangeRelationship bindingExchanges(Collection<AmqpExchange> destExchanges,
                                                 Collection<String> routingKeys) {
        if (CollectionUtils.isNotEmpty(destExchanges)) {
            destExchanges.forEach(dest -> {
                if (CollectionUtils.isNotEmpty(routingKeys))
                    routingKeys.forEach(routingKey ->
                            bindings.add(new Binding(exchange, dest, routingKey)));
                else
                    bindings.add(new Binding(exchange, dest));
            });
        }
        return this;
    }

    /**
     * @param destQueues AmqpQueue...
     * @return ExchangeRelationship
     */
    public ExchangeRelationship bindingQueues(AmqpQueue... destQueues) {
        return bindingQueues(newFastList(destQueues), null);
    }

    /**
     * @param destQueues  Collection<AmqpQueue>
     * @param routingKeys Collection<String>
     * @return ExchangeRelationship
     */
    public ExchangeRelationship bindingQueues(Collection<AmqpQueue> destQueues,
                                              Collection<String> routingKeys) {
        if (CollectionUtils.isNotEmpty(destQueues)) {
            destQueues.forEach(dest -> {
                if (CollectionUtils.isNotEmpty(routingKeys))
                    routingKeys.forEach(routingKey ->
                            bindings.add(new Binding(exchange, dest, routingKey)));
                else
                    bindings.add(new Binding(exchange, dest));
            });
        }
        return this;
    }

    @Override
    public String toString() {
        return JsonWriter.toJsonHasNulls(this);
    }

    public static void main(String[] args) {
        System.out.println(ExchangeRelationship.direct("TEST_DIRECT").setAutoDelete(true).setInternal(true));

        AmqpExchange exchange0 = AmqpExchange.direct("ABC");
        AmqpExchange exchange1 = AmqpExchange.direct("ABC");

        System.out.println(exchange0);
        System.out.println(exchange1);

        System.out.println(exchange0 == exchange1);
        System.out.println(exchange0.idempotent(exchange1));

        System.out.println(AmqpExchange.direct("ABC"));
        System.out.println(AmqpQueue.named("ABC"));
    }

}