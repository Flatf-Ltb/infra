package io.flatf.transport.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.flatf.transport.api.Subscriber;
import org.agrona.CloseHelper;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron 订阅者, 对应 ZMQ 中的 {@code ZmqSubscriber}.
 *
 * <p>支持在同一 channel 上订阅多个 stream. 消费者签名为 {@code BiConsumer<Integer, byte[]>},
 * 其中 {@code Integer} 为消息到达的 streamId.
 *
 * <p>{@link #subscribe()} 在调用线程中阻塞轮询, 通常应由专用线程驱动 (实现 {@link Runnable#run}).
 */
public final class AeronSubscriber extends AeronComponent implements Subscriber {

    private static final Logger log = getLogger(AeronSubscriber.class);

    private static final int FRAGMENT_LIMIT = 10;

    private final Subscription[] subscriptions;
    private final BiConsumer<Integer, byte[]> consumer;

    AeronSubscriber(@Nonnull AeronCfg cfg,
                    @Nonnull int[] streamIds,
                    @Nonnull BiConsumer<Integer, byte[]> consumer) {
        super(cfg);
        nonNull(streamIds, "streamIds");
        nonNull(consumer, "consumer");
        this.consumer = consumer;
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
        for (Subscription sub : subscriptions) {
            CloseHelper.quietClose(sub);
        }
    }

    /**
     * 阻塞轮询所有订阅的 stream, 直到 {@link #closeIgnoreException()} 被调用.
     */
    @Override
    public void subscribe() {
        FragmentAssembler[] assemblers = buildAssemblers();
        int idleCount = 0;
        while (isRunning.get()) {
            int workCount = 0;
            for (int i = 0; i < subscriptions.length; i++) {
                workCount += subscriptions[i].poll(assemblers[i], FRAGMENT_LIMIT);
            }
            if (workCount == 0) {
                // 简单退避: 避免空转吃满 CPU; 低延迟场景可替换为 BusySpinIdleStrategy
                if (++idleCount > 100) {
                    Thread.yield();
                    idleCount = 0;
                }
            } else {
                idleCount = 0;
            }
        }
        log.info("AeronSubscriber -> [{}] stopped", name);
    }

    @Override
    public void reconnect() {
        throw new UnsupportedOperationException("AeronSubscriber does not support reconnect");
    }

    @Override
    public void run() {
        subscribe();
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