package io.scalecube.aeron.examples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.logbuffer.FragmentHandler;
import io.scalecube.aeron.examples.AeronHelper;
import io.scalecube.aeron.examples.DynamicCompositeAgent;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.controlResponseChannel;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;

public class MultiSessionReplayReceiver {

    public static final int STREAM_ID1 = 1001;
    public static final int STREAM_ID2 = 1002;

    public static final String RECORDING_ENDPOINT = "localhost:30121";
    public static final String REPLAY_ENDPOINT = "localhost:20121";

    public static final long IMAGE_LIVENESS_TIMEOUT = TimeUnit.SECONDS.toNanos(3);

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static AeronArchive aeronArchive;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
//        SigInt.register(MultiSessionReplayReceiver::close);

        Path aeronPath = Paths.get(CommonContext.generateRandomDirName());

        mediaDriver = MediaDriver.launch(new Context()
                .aeronDirectoryName(aeronPath.toString())
                .imageLivenessTimeoutNs(IMAGE_LIVENESS_TIMEOUT)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .unicastFlowControlSupplier(new MinMulticastFlowControlSupplier()));

        aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(aeronPath.toString())
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage));

        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(
                        new ChannelUriStringBuilder()
                                .media(UDP_MEDIA)
                                .endpoint("localhost:8010")
                                .build())
                .controlResponseChannel(controlResponseChannel()));

        String aeronDirectoryName = mediaDriver.aeronDirectoryName();
        System.out.printf("### aeronDirectoryName: %s%n", aeronDirectoryName);

        long controlSessionId = aeronArchive.controlSessionId();
        System.out.printf("### controlSessionId: %s%n", controlSessionId);

        DynamicCompositeAgent compositeAgent = new DynamicCompositeAgent("dynamic-agent");

        compositeAgent.add(newAgent(STREAM_ID1));
        compositeAgent.add(newAgent(STREAM_ID2));

        Thread thread = AgentRunner.startOnThread(
                new AgentRunner(YieldingIdleStrategy.INSTANCE, throwable -> {
                }, null, compositeAgent));

        thread.join();

        System.out.println("Shutting down...");

        close();
    }

    private static Agent newAgent(int streamId) {
        return new Agent() {

            String liveChannel;
            RecordingDescriptor rd;
            Subscription subscription;
            final String replayChannel = computeReplayChannel(REPLAY_ENDPOINT);

            boolean liveImageAdded;

            boolean replaysAdded = false;
            final long replaysAddDeadline = System.currentTimeMillis();

            boolean bobRemoved = false;
            long bobReplayId = -1;

            boolean aliceRemoved = false;
            long aliceReplayId = -1;

            String bobReplayChannel = null;
            String aliceReplayChannel = null;

            int bobSessionId = -1;
            int aliceSessionId = -1;

            final FragmentAssembler liveFragmentAssembler =
                    new FragmentAssembler(dontPrintAsciiMessage());
            final FragmentAssembler bobFragmentAssembler = new FragmentAssembler(dontPrintAsciiMessage());
            final FragmentAssembler aliceFragmentAssembler =
                    new FragmentAssembler(dontPrintAsciiMessage());

            @Override
            public void onStart() {
                // recording descriptor / per STREAM_ID
                rd = AeronArchiveUtil.findLastRecording(aeronArchive, rd -> rd.streamId() == streamId);
                if (rd == null) {
                    throw new IllegalStateException("Cannot find recording descriptor");
                }
                System.out.printf("### found rd: %s%n", rd);

                // live channel / per STREAM_ID
                liveChannel = new ChannelUriStringBuilder()
                        .media(UDP_MEDIA)
                        .controlEndpoint(RECORDING_ENDPOINT)
                        .sessionId(rd.sessionId())
                        .build();

                // multi-destination control subscription / per STREAM_ID
                String controlChannel = new ChannelUriStringBuilder()
                        .media(UDP_MEDIA)
                        .controlMode(MDC_CONTROL_MODE_MANUAL)
                        .build();
                subscription = aeron.addSubscription(controlChannel, streamId);
                printSubscription(subscription);
            }

            @Override
            public String roleName() {
                return String.join("-", "agent", String.valueOf(streamId));
            }

            @Override
            public int doWork() {
                Image liveImage = subscription.imageBySessionId(rd.sessionId());

                final Image bobImage = bobSessionId != -1
                        ? subscription.imageBySessionId(bobSessionId) : null;
                final Image aliceImage = aliceSessionId != -1
                        ? subscription.imageBySessionId(aliceSessionId) : null;

                int progress = 0;

                if (!liveImageAdded) {
                    // add live destinaiton
                    subscription.asyncAddDestination(liveChannel);
                    liveImageAdded = true;
                }

                if (liveImage != null) {
                    progress += liveImage.poll(liveFragmentAssembler, 1);
                }

                if (bobImage != null) {
                    progress += bobImage.poll(bobFragmentAssembler, FRAGMENT_LIMIT);
                }

                if (aliceImage != null) {
                    progress += aliceImage.poll(aliceFragmentAssembler, FRAGMENT_LIMIT);
                }

                if (liveImage != null) {
                    long livePosition = liveImage.position();

                    if (!bobRemoved && bobImage != null && bobImage.position() == livePosition) {
                        // subscription.asyncRemoveDestination(bobReplayChannel);
                        aeronArchive.stopReplay(bobReplayId);
                        bobRemoved = true;
                        System.err.println(
                                "###" + Instant.now() + " | MERGED Bob channel | sessionId: " + bobSessionId);
                    }

                    if (!aliceRemoved && aliceImage != null && aliceImage.position() == livePosition) {
                        // subscription.asyncRemoveDestination(aliceReplayChannel);
                        aeronArchive.stopReplay(aliceReplayId);
                        aliceRemoved = true;
                        System.err.println(
                                "###" + Instant.now() + " | MERGED Alice channel  | sessionId: " + aliceSessionId);
                    }
                }

                if (!replaysAdded && replaysAddDeadline <= System.currentTimeMillis()) {
                    bobReplayId = aeronArchive
                            .startReplay(rd.recordingId(), 0, rd.stopPosition(), replayChannel, streamId);
                    bobReplayChannel = ChannelUri
                            .addSessionId(replayChannel, bobSessionId = (int) bobReplayId);
                    subscription.asyncAddDestination(bobReplayChannel);
                    System.err.println(
                            "###" + Instant.now() + " | ADDED Bob channel | sessionId: " + bobSessionId);

                    aliceReplayId = aeronArchive
                            .startReplay(rd.recordingId(), 0, rd.stopPosition(), replayChannel, streamId);
                    aliceReplayChannel = ChannelUri
                            .addSessionId(replayChannel, aliceSessionId = (int) aliceReplayId);
                    subscription.asyncAddDestination(aliceReplayChannel);

                    System.err.println(
                            "###" + Instant.now() + " | ADDED Alice channel | sessionId: " + aliceSessionId);

                    replaysAdded = true;
                }

                return progress;
            }
        };
    }

    private static String computeReplayChannel(String endpoint) {
        return new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                .controlEndpoint(endpoint)
                .build();
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }

    static FragmentHandler printAsciiMessage(final int streamId, final String agent) {
        return (buffer, offset, length, header) ->
                System.out.printf(
                        "<<%s>> | "
                                + "agent: %s, "
                                + "session: %d, "
                                + "stream: %d, "
                                + "initialTermId: %d, "
                                + "termId: %d, "
                                + "termOffset: %d%n",
                        buffer.getStringWithoutLengthAscii(offset, length),
                        agent,
                        header.sessionId(),
                        streamId,
                        header.initialTermId(),
                        header.termId(),
                        header.termOffset());
    }

    static FragmentHandler dontPrintAsciiMessage() {
        return (buffer, offset, length, header) -> {
            buffer.getStringWithoutLengthAscii(offset, length);
            // System.out.println(header.position());
        };
    }
}
