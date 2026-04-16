package io.scalecube.aeron.examples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.agent.EventLogAgent;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.logbuffer.FragmentHandler;
import io.scalecube.aeron.examples.AeronHelper;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.printAsciiMessage;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;

public class BasicReplayMergeReceiver {

    private static final String REPLAY_MERGE_CONTROL_ENDPOINT = "localhost:30121";
    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_LIMIT = 10;

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static AeronArchive aeronArchive;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) {
        // SigInt.register(BasicReplayMergeReceiver::close);

        System.setProperty("aeron.event.log", "admin");
        System.setProperty("aeron.event.archive.log", "all");
        EventLogAgent.agentmain("", ByteBuddyAgent.install());

        Path aeronPath = Paths.get(CommonContext.generateRandomDirName());

        mediaDriver = MediaDriver.launch(new Context()
                .aeronDirectoryName(aeronPath.toString()).spiesSimulateConnection(true));

        aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(aeronPath.toString())
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage));

        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(new ChannelUriStringBuilder()
                        .media(UDP_MEDIA)
                        .endpoint("localhost:8010")
                        .build())
                .controlResponseChannel(AeronHelper.controlResponseChannel()));

        String aeronDirectoryName = mediaDriver.aeronDirectoryName();
        System.out.printf("### aeronDirectoryName: %s%n", aeronDirectoryName);

        long controlSessionId = aeronArchive.controlSessionId();
        System.out.printf("### controlSessionId: %s%n", controlSessionId);

        RecordingDescriptor rd = AeronArchiveUtil
                .findLastRecording(aeronArchive, rd1 -> rd1.streamId() == STREAM_ID);
        if (rd == null) {
            throw new NoSuchElementException("recording not found");
        }

        // mds subscription
        Subscription subscription = aeron.addSubscription(
                new ChannelUriStringBuilder()
                        .media(UDP_MEDIA)
                        .controlMode(MDC_CONTROL_MODE_MANUAL)
                        // .sessionId(rd.sessionId)
                        .build(),
                STREAM_ID);

        printSubscription(subscription);

        String replayChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                // .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                // .controlEndpoint(REPLAY_MERGE_CONTROL_ENDPOINT)
                // .endpoint("localhost:0")
                .sessionId(rd.sessionId())
                .build();

        String replayDestination = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                // .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                // .controlEndpoint(REPLAY_MERGE_CONTROL_ENDPOINT)
                .endpoint("localhost:0")
                // .sessionId(rd.sessionId)
                .build();

        String liveDestination = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                // .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                .controlEndpoint(REPLAY_MERGE_CONTROL_ENDPOINT)
                .endpoint("localhost:0")
                // .sessionId(rd.sessionId)
                .build();

        ReplayMerge replayMerge = new ReplayMerge(
                subscription, aeronArchive, replayChannel,
                replayDestination, liveDestination, rd.recordingId(),
                rd.startPosition());

        final FragmentHandler fragmentHandler = printAsciiMessage(STREAM_ID);
        FragmentAssembler fragmentAssembler = new FragmentAssembler(fragmentHandler);
        SleepingMillisIdleStrategy idleStrategy = new SleepingMillisIdleStrategy(300);

        while (running.get()) {
            int progress = 0;
            progress += pollReplayMerge(replayMerge, fragmentHandler, fragmentAssembler);
            idleStrategy.idle(progress);
        }

        System.out.println("Shutting down...");

        close();
    }

    private static int pollReplayMerge(
            ReplayMerge replayMerge,
            FragmentHandler fragmentHandler,
            FragmentAssembler fragmentAssembler) {

        int progress;

        if (replayMerge.isMerged()) {
            final Image image = replayMerge.image();
            progress = image.poll(fragmentAssembler, FRAGMENT_LIMIT);
            if (image.isClosed()) {
                System.err.println("### replayMerge.image is closed, exiting");
                throw new RuntimeException("good bye");
            }
        } else {
            progress = replayMerge.poll(fragmentHandler, FRAGMENT_LIMIT);
            if (replayMerge.hasFailed()) {
                System.err.println("### replayMerge has failed, exiting");
                throw new RuntimeException("good bye");
            }
        }

        return progress;
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }

}
