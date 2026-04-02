package io.scalecube.aeron.examples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.agent.EventLogAgent;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.AeronArchive.AsyncConnect;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ControlEventListener;
import io.aeron.archive.client.RecordingSignalAdapter;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.TimeoutException;
import io.scalecube.aeron.examples.AeronHelper;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.controlResponseChannel;
import static io.scalecube.aeron.examples.AeronHelper.localControlChannel;
import static io.scalecube.aeron.examples.AeronHelper.printArchiveContext;

public class BasicReplicatorDestination2 {

    public static final String RECORDING_EVENTS_CHANNEL_ENDPOINT = "localhost:12030";
    public static final String CONTROL_CHANNEL_ENDPOINT = "localhost:12010";
    public static final String REPLICATION_CHANNEL_ENDPOINT = "localhost:0";
    public static final String LIVE_CONTROL_ENDPOINT = "localhost:30121";
    public static final String SRC_CONTROL_ENDPOINT = "localhost:8010";

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static Archive archive;
    private static AgentRunner agentRunner;

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
//        SigInt.register(BasicReplicatorDestination2::close);

        System.setProperty("aeron.event.log", "admin");
        System.setProperty("aeron.event.archive.log", "all");
        EventLogAgent.agentmain("", ByteBuddyAgent.install());

        Path aeronPath = Paths.get(CommonContext.generateRandomDirName());
        String instanceName = aeronPath.getFileName().toString();
        Path archivePath =
                AeronHelper.archivePath()
                        .orElseGet(() -> Paths.get(String.join("-", instanceName, "archive")));

        mediaDriver =
                MediaDriver.launch(
                        new Context().aeronDirectoryName(aeronPath.toString()).spiesSimulateConnection(true));

        aeron =
                Aeron.connect(
                        new Aeron.Context()
                                .aeronDirectoryName(aeronPath.toString())
                                .availableImageHandler(AeronHelper::printAvailableImage)
                                .unavailableImageHandler(AeronHelper::printUnavailableImage));

        archive =
                Archive.launch(
                        new Archive.Context()
                                .aeron(aeron)
                                .mediaDriverAgentInvoker(mediaDriver.sharedAgentInvoker())
                                .errorCounter(
                                        new AtomicCounter(
                                                mediaDriver.context().countersValuesBuffer(),
                                                SystemCounterDescriptor.ERRORS.id()))
                                .errorHandler(mediaDriver.context().errorHandler())
                                .archiveClientContext(
                                        new AeronArchive.Context().controlResponseChannel(controlResponseChannel()))
                                .localControlChannel(localControlChannel(instanceName))
                                .controlChannel(controlChannel())
                                .recordingEventsChannel(recordingEventsChannel())
                                .replicationChannel(replicationChannel())
                                .aeronDirectoryName(aeronPath.toString())
                                .archiveDirectoryName(archivePath.toString())
                                .threadingMode(ArchiveThreadingMode.SHARED));

        printArchiveContext(archive.context());

        agentRunner =
                new AgentRunner(
                        new SleepingMillisIdleStrategy(300),
                        System.err::println,
                        null,
                        new RecordingReplicationAgent());

        AgentRunner.startOnThread(agentRunner).join();

        close();
    }

    private static String replicationChannel() {
        return new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint(REPLICATION_CHANNEL_ENDPOINT)
                .build();
    }

    private static String recordingEventsChannel() {
        return new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                .controlEndpoint(RECORDING_EVENTS_CHANNEL_ENDPOINT)
                .build();
    }

    private static String controlChannel() {
        return new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint(CONTROL_CHANNEL_ENDPOINT)
                .build();
    }

    public static class RecordingReplicationAgent
            implements Agent, ControlEventListener, RecordingSignalConsumer {

        private final AeronArchive.Context dstContext;

        private final AeronArchive.Context srcContext;
        private State state;
        private AeronArchive dstClient;
        private AeronArchive srcClient;
        private AsyncConnect srcAsyncConnect;
        private long replicationId = NULL_VALUE;
        private RecordingSignalAdapter signalAdapter;
        private RecordingDescriptor srcRecordingDescriptor;
        private RecordingDescriptor dstRecordingDescriptor;
        private long dstRecordingPosition = NULL_VALUE;

        private enum State {
            INIT,
            RESET,
            POLL_CONNECT,
            CONNECTED,
            REPLICATE,
            POLL_REPLICATE,
            CLOSED
        }

        RecordingReplicationAgent() {
            dstContext =
                    new AeronArchive.Context()
                            .aeron(aeron)
                            .controlRequestChannel(controlChannel())
                            .controlResponseChannel(controlResponseChannel())
                            .recordingEventsChannel(recordingEventsChannel());
            srcContext =
                    new AeronArchive.Context()
                            .aeron(aeron)
                            .controlRequestChannel(srcControlChannel())
                            .controlResponseChannel(controlResponseChannel());
        }

        @Override
        public String roleName() {
            return getClass().getSimpleName();
        }

        @Override
        public void onStart() {
            dstClient = AeronArchive.connect(dstContext.clone());
            signalAdapter = newRecordingSignalAdapter(dstClient, this, this);
            dstRecordingDescriptor = findRecordingDescriptor(dstClient); // nullable
            dstRecordingPosition = dstRecordingDescriptor != null
                    ? dstRecordingDescriptor.stopPosition() : NULL_VALUE;
            state = State.INIT;
            System.out.println("start");
        }

        @Override
        public int doWork() {
            try {
                switch (state) {
                    case INIT -> {
                        return connect();
                    }
                    case POLL_CONNECT -> {
                        return pollConnect();
                    }
                    case CONNECTED -> {
                        return catchup();
                    }
                    case REPLICATE -> {
                        return replicate();
                    }
                    case POLL_REPLICATE -> {
                        return pollReplicate();
                    }
                    case RESET -> {
                        return reset();
                    }
                    default -> {
                    }
                    // no-op
                }
            } catch (Exception e) {
                System.err.println("exception occurred: " + e);
                state = State.RESET;
                return 0;
            }
            throw new AgentTerminationException(state + " state is not supported");
        }

        @Override
        public void onClose() {
            reset();
            CloseHelper.close(dstClient);
            state = State.CLOSED;
            System.out.println("closed");
        }

        private int reset() {
            System.out.println("reset");

            if (replicationId != NULL_VALUE) {
                dstClient.stopReplication(replicationId);
            }

            CloseHelper.close(srcClient);
            CloseHelper.close(srcAsyncConnect);
            state = State.INIT;
            return 0;
        }

        private int connect() {
            System.out.println("connect");
            srcAsyncConnect = AeronArchive.asyncConnect(srcContext.clone());
            state = State.POLL_CONNECT;
            return 1;
        }

        private int pollConnect() {
            try {
                final int step = srcAsyncConnect.step();
                srcClient = srcAsyncConnect.poll();

                if (srcClient != null) {
                    srcAsyncConnect = null;
                    state = State.CONNECTED;
                    System.out.println("connected");
                    return 1;
                }

                if (srcAsyncConnect.step() != step) {
                    return 1;
                }
            } catch (TimeoutException ex) {
                state = State.RESET;
            }
            return 0;
        }

        private int catchup() {
            if (srcRecordingDescriptor == null) {
                srcRecordingDescriptor = findRecordingDescriptorOrThrow(srcClient);
            }

            long srcPosition = srcClient.getRecordingPosition(srcRecordingDescriptor.recordingId());

            System.out.printf(
                    "[catchup] dstPosition: %d, srcPosition: %d%n", dstRecordingPosition, srcPosition);

            if (dstRecordingPosition <= srcPosition) {
                state = State.REPLICATE;
                return 1;
            }

            return 0;
        }

        private int replicate() {
            long srcRecordingId = srcRecordingDescriptor.recordingId();
            long dstRecordingId =
                    dstRecordingDescriptor != null ? dstRecordingDescriptor.recordingId() : NULL_VALUE;

            replicationId =
                    dstClient.replicate(
                            srcRecordingId,
                            dstRecordingId,
                            srcClient.context().controlRequestStreamId(),
                            srcClient.context().controlRequestChannel(),
                            new ChannelUriStringBuilder()
                                    .media(UDP_MEDIA)
                                    .controlEndpoint(LIVE_CONTROL_ENDPOINT)
                                    .endpoint("localhost:0")
                                    .build());

            state = State.POLL_REPLICATE;
            System.out.printf(
                    "replicate, replicationId: %d, srcRecordingId: %d, dstRecordingId: %d%n",
                    replicationId, srcRecordingId, dstRecordingId);
            return 1;
        }

        private int pollReplicate() {
            return signalAdapter.poll();
        }

        @Override
        public void onResponse(long controlSessionId,
                               long correlationId,
                               long relevantId,
                               ControlResponseCode code,
                               String errorMessage) {

            System.out.printf("[control][onResponse] "
                            + "controlSessionId: %d, "
                            + "correlationId: %d, "
                            + "relevantId: %d, "
                            + "code: %s, "
                            + "errorMessage: '%s', "
                            + "thread: %s"
                            + "%n",
                    controlSessionId,
                    correlationId,
                    relevantId,
                    code,
                    errorMessage,
                    Thread.currentThread().getName());

            if (code == ControlResponseCode.ERROR) {
                replicationId = NULL_VALUE;
                state = State.RESET;
                throw new ArchiveException(errorMessage, (int) relevantId, correlationId);
            }
        }

        @Override
        public void onSignal(
                long controlSessionId,
                long correlationId,
                long recordingId,
                long subscriptionId,
                long position,
                RecordingSignal signal) {

            System.out.printf(
                    "[control][onSignal] "
                            + "controlSessionId: %d, "
                            + "correlationId: %d, "
                            + "recordingId: %d, "
                            + "subscriptionId: %s, "
                            + "position: %s, "
                            + "signal: %s, "
                            + "thread: %s"
                            + "%n",
                    controlSessionId,
                    correlationId,
                    recordingId,
                    subscriptionId,
                    position,
                    signal,
                    Thread.currentThread().getName());

            dstRecordingPosition = position;

            if (signal == RecordingSignal.REPLICATE) {
                dstRecordingDescriptor = findRecordingDescriptor(dstClient);
                System.out.println("### replicated dstRecordingDescriptor: " + dstRecordingDescriptor);
            }

            if (signal == RecordingSignal.STOP) {
                replicationId = NULL_VALUE;
                state = State.RESET;
            }
        }

        private static RecordingDescriptor findRecordingDescriptorOrThrow(AeronArchive archiveClient) {
            return Optional.ofNullable(findRecordingDescriptor(archiveClient))
                    .orElseThrow(() -> new IllegalStateException("expected recording descriptor not found"));
        }

        private static RecordingDescriptor findRecordingDescriptor(AeronArchive archiveClient) {
            return AeronArchiveUtil.findLastRecording(archiveClient, rd -> rd.streamId() == STREAM_ID);
        }

        private static RecordingSignalAdapter newRecordingSignalAdapter(
                AeronArchive aeronArchive,
                ControlEventListener controlEventListener,
                RecordingSignalConsumer recordingSignalConsumer) {
            return new RecordingSignalAdapter(
                    aeronArchive.controlSessionId(),
                    controlEventListener,
                    recordingSignalConsumer,
                    aeronArchive.controlResponsePoller().subscription(),
                    FRAGMENT_LIMIT);
        }
    }

    private static String srcControlChannel() {
        return new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(SRC_CONTROL_ENDPOINT).build();
    }

    private static void close() {
        CloseHelper.close(agentRunner);
        CloseHelper.close(archive);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
