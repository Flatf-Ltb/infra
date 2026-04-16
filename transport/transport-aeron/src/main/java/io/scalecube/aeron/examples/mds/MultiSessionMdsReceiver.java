package io.scalecube.aeron.examples.mds;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
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

public class MultiSessionMdsReceiver {

    public static final String RECORDING_ENDPOINT = "localhost:30121";
    public static final String REPLAY_BOB_ENDPOINT = "localhost:20121";
    public static final String REPLAY_ALICE_ENDPOINT = "localhost:20122";

    public static final int RECORDING_SESSION_ID = 100500;
    public static final int REPLAY_BOB_SESSION_ID = 100600;
    public static final int REPLAY_ALICE_SESSION_ID = 100700;

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

//        SigInt.register(MultiSessionMdsReceiver::close);

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
                .rejoin(false)
                .build();

        Subscription subscription = aeron.addSubscription(controlChannel, STREAM_ID);
        printSubscription(subscription);

        String liveChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlEndpoint(RECORDING_ENDPOINT)
                .endpoint("localhost:0")
                .sessionId(RECORDING_SESSION_ID)
                .build();

        String bobChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlEndpoint(REPLAY_BOB_ENDPOINT)
                .endpoint("localhost:0")
                .sessionId(REPLAY_BOB_SESSION_ID)
                .build();

        String aliceChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlEndpoint(REPLAY_ALICE_ENDPOINT)
                .endpoint("localhost:0")
                .sessionId(REPLAY_ALICE_SESSION_ID)
                .build();

        SleepingMillisIdleStrategy idleStrategy = new SleepingMillisIdleStrategy(300);
        final FragmentHandler fragmentHandler = printAsciiMessage(STREAM_ID);
        FragmentAssembler fragmentAssembler = new FragmentAssembler(fragmentHandler);

        boolean isLiveAdded = false;
        boolean isBobAdded = false;
        boolean isAliceAdded = false;

        long liveAddStart = System.currentTimeMillis();
        long bobAddStart = -1;
        long aliceAddStart = -1;

        while (running.get()) {
            idleStrategy.idle(subscription.poll(fragmentAssembler, FRAGMENT_LIMIT));

            if (!isLiveAdded && (System.currentTimeMillis() - liveAddStart) >= 5000) {
                subscription.addDestination(liveChannel);
                isLiveAdded = true;
                bobAddStart = System.currentTimeMillis();
                System.err.println("### " + Instant.now() + "| ADDED Live channel");
            }

            if (isLiveAdded && !isBobAdded && (System.currentTimeMillis() - bobAddStart) >= 5000) {
                subscription.addDestination(bobChannel);
                isBobAdded = true;
                aliceAddStart = System.currentTimeMillis();
                System.err.println("### " + Instant.now() + "| ADDED Bob channel");
            }

            if (isLiveAdded && isBobAdded && !isAliceAdded
                    && (System.currentTimeMillis() - aliceAddStart) >= 5000) {
                subscription.addDestination(aliceChannel);
                isAliceAdded = true;
                System.err.println("### " + Instant.now() + "| ADDED Alice channel");
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
