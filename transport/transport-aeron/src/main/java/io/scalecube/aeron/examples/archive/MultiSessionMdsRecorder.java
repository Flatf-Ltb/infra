package io.scalecube.aeron.examples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.scalecube.aeron.examples.AeronHelper;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.awaitRecordingPosCounter;
import static io.scalecube.aeron.examples.AeronHelper.controlResponseChannel;
import static io.scalecube.aeron.examples.AeronHelper.localControlChannel;
import static io.scalecube.aeron.examples.AeronHelper.printArchiveContext;
import static io.scalecube.aeron.examples.AeronHelper.printRecordedPublication;

public class MultiSessionMdsRecorder {

    public static final int STREAM_ID1 = 1001;
    public static final int STREAM_ID2 = 1002;

    public static final String RECORDING_ENDPOINT = "localhost:30121";
    public static final String RECORDING_EVENTS_CHANNEL_ENDPOINT = "localhost:8030";
    public static final String CONTROL_CHANNEL_ENDPOINT = "localhost:8010";
    public static final String REPLICATION_CHANNEL_ENDPOINT = "localhost:0";

    public static final int RECORDING_SESSION_ID1 = 100500;
    public static final int RECORDING_SESSION_ID2 = 100511;

    public static final long IMAGE_LIVENESS_TIMEOUT = TimeUnit.SECONDS.toNanos(3);

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static Archive archive;
    private static AeronArchive aeronArchive;
    private static Publication publication1;
    private static Publication publication2;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    //@SuppressWarnings("checkstyle:Indentation")
    public static void main(String[] args) throws InterruptedException {
        // SigInt.register(MultiSessionMdsRecorder::close);

        Path aeronPath = Paths.get(CommonContext.generateRandomDirName());
        String instanceName = aeronPath.getFileName().toString();
        Path archivePath =
                AeronHelper.archivePath()
                        .orElseGet(() -> Paths.get(String.join("-", instanceName, "archive")));

        mediaDriver =
                MediaDriver.launch(
                        new MediaDriver.Context()
                                .aeronDirectoryName(aeronPath.toString())
                                .imageLivenessTimeoutNs(IMAGE_LIVENESS_TIMEOUT)
                                .ipcTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                                .unicastFlowControlSupplier(new MinMulticastFlowControlSupplier()));

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
                                .localControlChannel(localControlChannel(instanceName))
                                .controlChannel(controlChannel())
                                .recordingEventsChannel(recordingEventsChannel())
                                .replicationChannel(replicationChannel())
                                .aeronDirectoryName(aeronPath.toString())
                                .archiveDirectoryName(archivePath.toString())
                                .threadingMode(ArchiveThreadingMode.SHARED));

        printArchiveContext(archive.context());

        CountersReader counters = aeron.countersReader();

        aeronArchive =
                AeronArchive.connect(
                        new AeronArchive.Context()
                                .aeron(aeron)
                                .controlResponseChannel(controlResponseChannel()));

        long controlSessionId = aeronArchive.controlSessionId();
        System.out.printf("### controlSessionId: %s%n", controlSessionId);

        // multi-destination publication / STREAM_ID1
        publication1 =
                aeronArchive.addRecordedExclusivePublication(
                        new ChannelUriStringBuilder()
                                .media(UDP_MEDIA)
                                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                                .controlEndpoint(RECORDING_ENDPOINT)
                                .sessionId(RECORDING_SESSION_ID1)
                                .build(),
                        STREAM_ID1);

        // multi-destination publication / STREAM_ID2
        publication2 =
                aeronArchive.addRecordedExclusivePublication(
                        new ChannelUriStringBuilder()
                                .media(UDP_MEDIA)
                                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                                .controlEndpoint(RECORDING_ENDPOINT)
                                .sessionId(RECORDING_SESSION_ID2)
                                .build(),
                        STREAM_ID2);

        int recordingPosCounterId1 = awaitRecordingPosCounter(counters, publication1.sessionId());
        long recordingId1 = RecordingPos.getRecordingId(counters, recordingPosCounterId1);

        int recordingPosCounterId2 = awaitRecordingPosCounter(counters, publication2.sessionId());
        long recordingId2 = RecordingPos.getRecordingId(counters, recordingPosCounterId2);

        printRecordedPublication(aeronArchive, publication1, recordingPosCounterId1, recordingId1);
        printRecordedPublication(aeronArchive, publication2, recordingPosCounterId2, recordingId2);

        //noinspection InfiniteLoopStatement
        for (long i = 0; ; i++) {
            sendMessage(publication1, i);
            sendMessage(publication2, i);
            // Thread.yield();
            //noinspection BusyWait
            Thread.sleep(1);
        }
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

    private static void close() {
        if (running != null) {
            running.set(false);
        }
        CloseHelper.close(publication1);
        CloseHelper.close(publication2);
        CloseHelper.close(aeron);
        CloseHelper.close(aeronArchive);
        CloseHelper.close(archive);
        CloseHelper.close(mediaDriver);
    }

    static void sendMessage(Publication publication, long i) {
        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
        final int length = buffer.putStringWithoutLengthAscii(0, "Hello World! " + i);
        long result = publication.offer(buffer, 0, length);
        verifyResult(result);
    }

    static void verifyResult(long result) {
        if (result == Publication.CLOSED) {
            throw new RuntimeException("Offer failed because publication is closed");
        }
        if (result == Publication.MAX_POSITION_EXCEEDED) {
            throw new RuntimeException("Offer failed due to publication reaching its max position");
        }
    }
}
