package io.scalecube.aeron.examples.mds;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.agent.EventLogAgent;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.scalecube.aeron.examples.AeronHelper;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printAsciiMessage;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;

public class SingleSessionMdsReceiver {

    public static final String REPLAY_ENDPOINT = "localhost:20121";
    public static final String CONTROL_ENDPOINT = "localhost:30121";
    public static final int SESSION_ID = 100500;
    public static final long IMAGE_LIVENESS_TIMEOUT = TimeUnit.SECONDS.toNanos(3);

    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static MediaDriver mediaDriver;
    private static Aeron aeron;

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) {
        System.setProperty("aeron.event.log", "admin");
        System.setProperty("aeron.event.archive.log", "all");
        EventLogAgent.agentmain("", ByteBuddyAgent.install());

//        SigInt.register(SingleSessionMdsReceiver::close);

        mediaDriver = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                        .spiesSimulateConnection(true)
                        .imageLivenessTimeoutNs(IMAGE_LIVENESS_TIMEOUT));

        String aeronDirectoryName = mediaDriver.aeronDirectoryName();

        Context context = new Context()
                .aeronDirectoryName(aeronDirectoryName)
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage);

        aeron = Aeron.connect(context);
        System.out.println("hello, " + context.aeronDirectoryName());

        String controlChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlMode(MDC_CONTROL_MODE_MANUAL)
                .sessionId(SESSION_ID)
                .rejoin(false)
                .build();

        Subscription subscription = aeron.addSubscription(controlChannel, STREAM_ID);

        printSubscription(subscription);

        String liveChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlEndpoint(CONTROL_ENDPOINT)
                .endpoint("localhost:0")
                .build();

        String replayChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint(REPLAY_ENDPOINT)
                .sessionId(SESSION_ID)
                .build();

        // subscription.addDestination(liveChannel);
        // subscription.addDestination(replayChannel);

        SleepingMillisIdleStrategy idleStrategy = new SleepingMillisIdleStrategy(100);
        final FragmentHandler fragmentHandler = printAsciiMessage(STREAM_ID);
        FragmentAssembler fragmentAssembler = new FragmentAssembler(fragmentHandler);

        boolean isReplayAdded = false;
        boolean isLiveAdded = false;
        boolean isReplayRemoved = false;

        long replayAddStart = System.currentTimeMillis();
        long liveAddStart = -1;
        long replayRemoveStart = -1;
        Image lastImage = null;

        while (running.get()) {
            Image image = subscription.imageCount() == 1 ? subscription.imageAtIndex(0) : null;
            boolean imageHasActiveTransports = image != null && image.activeTransportCount() >= 2;

            if (image != null) {
                if (lastImage == null) {
                    lastImage = image;
                } else if (lastImage != image) {
                    System.err.println("### " + Instant.now() + "| Second image detected, good bye");
                    break;
                }
            }

            if (image != null && (image.isClosed() || image.isEndOfStream())) {
                System.err.println("### " + Instant.now() + "| Image is not usable, good bye");
                break;
            }

            idleStrategy.idle(subscription.poll(fragmentAssembler, FRAGMENT_LIMIT));

            if (!isReplayAdded && (System.currentTimeMillis() - replayAddStart) >= 5000) {
                subscription.addDestination(replayChannel);
                isReplayAdded = true;
                liveAddStart = System.currentTimeMillis();
                System.err.println("### " + Instant.now() + "| ADDED replayChannel");
            }

            if (isReplayAdded
                    && !isLiveAdded
                    && image != null
                    && (System.currentTimeMillis() - liveAddStart) >= 5000) {
                subscription.addDestination(liveChannel);
                isLiveAdded = true;
                replayRemoveStart = System.currentTimeMillis();
                System.err.println("### " + Instant.now() + "| ADDED liveChannel");
            }

            if (isReplayAdded
                    && isLiveAdded
                    && !isReplayRemoved
                    && imageHasActiveTransports
                    && (System.currentTimeMillis() - replayRemoveStart) >= 5000) {
                subscription.removeDestination(replayChannel);
                isReplayRemoved = true;
                System.err.println("### " + Instant.now() + "| REMOVED replayChannel");
            }
        }

        System.out.println("Shutting down...");

        close();
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(mediaDriver);
        CloseHelper.close(aeron);
    }
}
