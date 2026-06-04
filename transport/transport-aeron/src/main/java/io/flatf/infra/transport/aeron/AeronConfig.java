package io.flatf.infra.transport.aeron;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.flatf.common.serialization.specific.BytesSerializer;
import io.flatf.foundation.transport.TransportConfig;
import io.flatf.foundation.transport.api.IndexedMessageConsumer;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static io.flatf.common.lang.Validator.nonEmpty;
import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron transport configuration and factory.
 */
public final class AeronConfig implements TransportConfig {

    private static final Logger log = getLogger(AeronConfig.class);

    private static final int DEFAULT_FRAGMENT_LIMIT = 10;
    private static final int DEFAULT_PUBLISH_RETRY_COUNT = 3;
    private static final Object SHARED_DRIVER_LOCK = new Object();

    private static MediaDriver sharedDriver;
    private static int sharedEmbeddedClientCount;

    /**
     * Same-host IPC transport backed by an embedded shared MediaDriver.
     */
    public static AeronConfig ipc(int streamId) {
        validateStreamId(streamId);
        return new AeronConfig(
            AeronChannel.ipc(),
            streamId,
            true,
            DEFAULT_FRAGMENT_LIMIT,
            DEFAULT_PUBLISH_RETRY_COUNT,
            defaultIdleStrategyFactory()
        );
    }

    /**
     * UDP unicast transport backed by an external MediaDriver.
     */
    public static AeronConfig udp(@Nonnull String host, int port, int streamId) {
        nonEmpty(host, "host");
        validatePort(port);
        validateStreamId(streamId);
        return new AeronConfig(
            AeronChannel.udp(host, port),
            streamId,
            false,
            DEFAULT_FRAGMENT_LIMIT,
            DEFAULT_PUBLISH_RETRY_COUNT,
            defaultIdleStrategyFactory()
        );
    }

    private final AeronChannel channel;
    private final int streamId;
    private final boolean embeddedDriver;
    private final int fragmentLimit;
    private final int publishRetryCount;
    private final Supplier<IdleStrategy> subscriberIdleStrategyFactory;

    private AeronConfig(AeronChannel channel,
                        int streamId,
                        boolean embeddedDriver,
                        int fragmentLimit,
                        int publishRetryCount,
                        Supplier<IdleStrategy> subscriberIdleStrategyFactory) {
        this.channel = channel;
        this.streamId = streamId;
        this.embeddedDriver = embeddedDriver;
        this.fragmentLimit = fragmentLimit;
        this.publishRetryCount = publishRetryCount;
        this.subscriberIdleStrategyFactory = subscriberIdleStrategyFactory;
    }

    AeronChannel getChannel() {
        return channel;
    }

    int getStreamId() {
        return streamId;
    }

    int getFragmentLimit() {
        return fragmentLimit;
    }

    int getPublishRetryCount() {
        return publishRetryCount;
    }

    IdleStrategy newSubscriberIdleStrategy() {
        return subscriberIdleStrategyFactory.get();
    }

    public AeronConfig withFragmentLimit(int fragmentLimit) {
        if (fragmentLimit <= 0) throw new IllegalArgumentException("fragmentLimit must be positive");
        return new AeronConfig(
            channel,
            streamId,
            embeddedDriver,
            fragmentLimit,
            publishRetryCount,
            subscriberIdleStrategyFactory
        );
    }

    public AeronConfig withPublishRetryCount(int publishRetryCount) {
        if (publishRetryCount <= 0) throw new IllegalArgumentException("publishRetryCount must be positive");
        return new AeronConfig(
            channel,
            streamId,
            embeddedDriver,
            fragmentLimit,
            publishRetryCount,
            subscriberIdleStrategyFactory
        );
    }

    public AeronConfig withSubscriberIdleStrategyFactory(@Nonnull Supplier<IdleStrategy> idleStrategyFactory) {
        nonNull(idleStrategyFactory, "idleStrategyFactory");
        return new AeronConfig(
            channel,
            streamId,
            embeddedDriver,
            fragmentLimit,
            publishRetryCount,
            idleStrategyFactory
        );
    }

    Aeron newAeron() {
        Aeron.Context context = new Aeron.Context();
        if (embeddedDriver) {
            MediaDriver driver = acquireSharedDriver();
            context.aeronDirectoryName(driver.aeronDirectoryName());
            log.debug("Aeron client connecting to embedded driver dir: {}", driver.aeronDirectoryName());
        }
        return Aeron.connect(context);
    }

    void onAeronClosed() {
        if (embeddedDriver) {
            releaseSharedDriver();
        }
    }

    public <T> AeronPublisher<T> createPublisher(@Nonnull BytesSerializer<T> serializer) {
        nonNull(serializer, "serializer");
        return new AeronPublisher<>(this, serializer);
    }

    public AeronSubscriber createSubscriber(@Nonnull int[] streamIds,
                                            @Nonnull BiConsumer<Integer, byte[]> consumer) {
        nonNull(streamIds, "streamIds");
        nonNull(consumer, "consumer");
        return new AeronSubscriber(this, streamIds, consumer);
    }

    public AeronZeroCopySubscriber createZeroCopySubscriber(
        @Nonnull int[] streamIds,
        @Nonnull IndexedMessageConsumer<AeronMessageView> consumer) {
        nonNull(streamIds, "streamIds");
        nonNull(consumer, "consumer");
        return new AeronZeroCopySubscriber(this, streamIds, consumer);
    }

    @Override
    public String connectionInfo() {
        return channel.uri() + "#" + streamId;
    }

    @Override
    public String getConfigInfo() {
        return "AeronCfg{channel=" + channel.uri()
            + ", streamId=" + streamId
            + ", embedded=" + embeddedDriver
            + ", fragmentLimit=" + fragmentLimit
            + ", publishRetryCount=" + publishRetryCount + "}";
    }

    @Override
    public String toString() {
        return getConfigInfo();
    }

    private static Supplier<IdleStrategy> defaultIdleStrategyFactory() {
        return () -> new BackoffIdleStrategy(1, 10, 1, 1_000);
    }

    private static MediaDriver acquireSharedDriver() {
        synchronized (SHARED_DRIVER_LOCK) {
            if (sharedDriver == null) {
                sharedDriver = MediaDriver.launchEmbedded();
                log.info("Embedded MediaDriver started -> {}", sharedDriver.aeronDirectoryName());
            }
            sharedEmbeddedClientCount++;
            return sharedDriver;
        }
    }

    private static void releaseSharedDriver() {
        synchronized (SHARED_DRIVER_LOCK) {
            if (sharedEmbeddedClientCount > 0) {
                sharedEmbeddedClientCount--;
            }
            if (sharedEmbeddedClientCount == 0 && sharedDriver != null) {
                log.info("Embedded MediaDriver stopping -> {}", sharedDriver.aeronDirectoryName());
                CloseHelper.quietClose(sharedDriver);
                sharedDriver = null;
            }
        }
    }

    private static void validatePort(int port) {
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
    }

    private static void validateStreamId(int streamId) {
        if (streamId <= 0) {
            throw new IllegalArgumentException("streamId must be positive");
        }
    }
}
