package io.flatf.transport.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.flatf.transport.api.Subscriber;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron subscriber capable of polling multiple streams on the same channel.
 */
public final class AeronSubscriber extends AeronComponent implements Subscriber {

    private static final Logger log = getLogger(AeronSubscriber.class);

    private final Subscription[] subscriptions;
    private final BiConsumer<Integer, byte[]> consumer;
    private final IdleStrategy idleStrategy;
    private final int fragmentLimit;

    AeronSubscriber(@Nonnull AeronConfig cfg,
                    @Nonnull int[] streamIds,
                    @Nonnull BiConsumer<Integer, byte[]> consumer) {
        super(cfg);
        nonNull(streamIds, "streamIds");
        nonNull(consumer, "consumer");
        if (streamIds.length == 0) {
            throw new IllegalArgumentException("streamIds must not be empty");
        }
        this.consumer = consumer;
        this.fragmentLimit = cfg.getFragmentLimit();
        this.idleStrategy = cfg.newSubscriberIdleStrategy();
        this.subscriptions = new Subscription[streamIds.length];
        for (int i = 0; i < streamIds.length; i++) {
            subscriptions[i] = aeron.addSubscription(cfg.getChannel().uri(), streamIds[i]);
        }
        this.name = "ASub$" + cfg.getChannel() + Arrays.toString(streamIds);
        newStartTime();
        log.info("AeronSubscriber created -> {}", name);
    }

    @Override
    public AeronType getAeronType() {
        return AeronType.AERON_SUBSCRIBER;
    }

    @Override
    protected void closeResources() {
        for (Subscription subscription : subscriptions) {
            CloseHelper.quietClose(subscription);
        }
    }

    @Override
    protected boolean isTransportConnected() {
        for (Subscription subscription : subscriptions) {
            if (subscription != null && !subscription.isClosed() && subscription.isConnected()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void subscribe() {
        FragmentAssembler[] assemblers = buildAssemblers();
        while (isRunning.get()) {
            int workCount = 0;
            for (int i = 0; i < subscriptions.length; i++) {
                workCount += subscriptions[i].poll(assemblers[i], fragmentLimit);
            }
            idleStrategy.idle(workCount);
        }
        log.info("AeronSubscriber -> [{}] stopped", name);
    }

    @Override
    public void reconnect() {
        throw new UnsupportedOperationException("AeronSubscriber does not support reconnect");
    }

    @Override
    public boolean reconnectSupported() {
        return false;
    }

    @Override
    public void run() {
        try {
            subscribe();
        } catch (RuntimeException e) {
            log.error("AeronSubscriber -> [{}] terminated unexpectedly", name, e);
            throw e;
        }
    }

    private FragmentAssembler[] buildAssemblers() {
        FragmentAssembler[] assemblers = new FragmentAssembler[subscriptions.length];
        for (int i = 0; i < subscriptions.length; i++) {
            final int streamId = subscriptions[i].streamId();
            final FragmentHandler handler = (buffer, offset, length, header) -> {
                byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                consumer.accept(streamId, bytes);
            };
            assemblers[i] = new FragmentAssembler(handler);
        }
        return assemblers;
    }
}
