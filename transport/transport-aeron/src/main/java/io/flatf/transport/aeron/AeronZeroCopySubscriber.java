package io.flatf.transport.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.flatf.transport.api.IndexedMessageConsumer;
import io.flatf.transport.api.Subscriber;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.Arrays;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron subscriber variant that exposes payloads through a callback-scoped zero-copy view.
 */
public final class AeronZeroCopySubscriber extends AeronComponent implements Subscriber {

    private static final Logger log = getLogger(AeronZeroCopySubscriber.class);

    private final Subscription[] subscriptions;
    private final AeronMessageView[] messageViews;
    private final IndexedMessageConsumer<AeronMessageView> consumer;
    private final IdleStrategy idleStrategy;
    private final int fragmentLimit;

    AeronZeroCopySubscriber(@Nonnull AeronConfig cfg,
                            @Nonnull int[] streamIds,
                            @Nonnull IndexedMessageConsumer<AeronMessageView> consumer) {
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
        this.messageViews = new AeronMessageView[streamIds.length];
        for (int i = 0; i < streamIds.length; i++) {
            subscriptions[i] = aeron.addSubscription(cfg.getChannel().uri(), streamIds[i]);
            messageViews[i] = new AeronMessageView();
        }
        this.name = "AZSub$" + cfg.getChannel() + Arrays.toString(streamIds);
        newStartTime();
        log.info("AeronZeroCopySubscriber created -> {}", name);
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
        log.info("AeronZeroCopySubscriber -> [{}] stopped", name);
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
            log.error("AeronZeroCopySubscriber -> [{}] terminated unexpectedly", name, e);
            throw e;
        }
    }

    private FragmentAssembler[] buildAssemblers() {
        FragmentAssembler[] assemblers = new FragmentAssembler[subscriptions.length];
        for (int i = 0; i < subscriptions.length; i++) {
            final int index = i;
            final int streamId = subscriptions[i].streamId();
            final FragmentHandler handler = (buffer, offset, length, header) ->
                    consumer.accept(streamId, messageViews[index].wrap(buffer, offset, length));
            assemblers[i] = new FragmentAssembler(handler);
        }
        return assemblers;
    }
}
