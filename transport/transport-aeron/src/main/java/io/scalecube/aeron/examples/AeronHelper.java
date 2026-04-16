package io.scalecube.aeron.examples;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.FragmentHandler;
import io.scalecube.aeron.examples.meter.ThroughputMeter;
import org.agrona.BufferUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.StringJoiner;

import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.UDP_MEDIA;

public class AeronHelper {

    public static final FragmentHandler NOOP_FRAGMENT_HANDLER = (buffer, offset, length, header) -> {
    };

    public static final int STREAM_ID = 1001;
    public static final int NUMBER_OF_MESSAGES = (int) 1e6;
    public static final int FRAGMENT_LIMIT = 100;
    public static final int MESSAGE_LENGTH = 256;
    public static final int RUNS = 10;
    public static final int MTU65K = 65504;
    public static final double MEGABYTE = 1024.0d * 1024.0d;
    public static final int HEADER_LENGTH = 32;

    /**
     * Returns value under {@code ARCHIVE_PATH} env variable.
     *
     * @return result
     */
    public static Optional<Path> archivePath() {
        return Optional.ofNullable(System.getenv("ARCHIVE_PATH"))
                .flatMap(str -> {
                    Path path = Paths.get(str);
                    File file = path.toFile().getAbsoluteFile();
                    return file.exists() ? Optional.of(path) : Optional.empty();
                });
    }

    /**
     * Awaits recording.
     *
     * @param counters  counters
     * @param sessionId sessionId
     * @return result
     */
    public static int awaitRecordingPosCounter(CountersReader counters, int sessionId) {
        final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (CountersReader.NULL_COUNTER_ID == counterId) {
            idleStrategy.idle();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        }
        return counterId;
    }

    /**
     * Prints recorded publication.
     *
     * @param aeronArchive          aeronArchive
     * @param publication           publication
     * @param recordingPosCounterId recordingPosCounterId
     * @param recordingId           recordingId
     */
    public static void printRecordedPublication(
            AeronArchive aeronArchive,
            Publication publication,
            int recordingPosCounterId,
            long recordingId) {
        int recordingSubscriptions = aeronArchive.listRecordingSubscriptions(
                0,
                Integer.MAX_VALUE,
                "",
                STREAM_ID,
                true,
                (csid, cid, subscriptionId, streamId, strippedChannel) ->
                        System.out.printf(
                                "[listRecordingSubscriptions] "
                                + "controlSessionId: %d, "
                                + "correlationId: %d, "
                                + "subscriptionId: %d, "
                                + "streamId: %d, "
                                + "strippedChannel: %s%n",
                                csid, cid, subscriptionId, streamId, strippedChannel));

        System.out.printf(
                "Recorded publication, "
                + "channel: %s, "
                + "streamId: %d, "
                + "session: %d, "
                + "recordingId: %d, "
                + "recordingPosCounterId: %d, "
                + "recordingSubscriptions(per stream): %d"
                + "%n",
                publication.channel(),
                publication.streamId(),
                publication.sessionId(),
                recordingId,
                recordingPosCounterId,
                recordingSubscriptions);
    }

    /**
     * Prints image info.
     *
     * @param image image
     */
    public static void printAvailableImage(final Image image) {
        try (final Subscription subscription = image.subscription()) {
            System.out.printf(
                    "### %s | [subscription:%d] Available image on %s "
                    + "streamId=%d, sessionId=%d, subscription.imageCount=%d from %s%n",
                    System.nanoTime(),
                    subscription.registrationId(),
                    subscription.channel(),
                    subscription.streamId(),
                    image.sessionId(),
                    subscription.imageCount(),
                    image.sourceIdentity());
        }
    }

    /**
     * Image handler.
     *
     * @param image image
     */
    public static void printUnavailableImage(final Image image) {
        final Subscription subscription = image.subscription();
        System.out.printf(
                "### %s | "
                + "[subscription:%d] "
                + "Unavailable image on %s "
                + "streamId=%d, sessionId=%d, isClosed=%s, subscription.imageCount=%d, isEndOfStream=%s%n",
                System.nanoTime(),
                subscription.registrationId(),
                subscription.channel(),
                subscription.streamId(),
                image.sessionId(),
                image.isClosed(),
                subscription.imageCount(),
                image.isEndOfStream());
    }

    /**
     * Prints archive driver context.
     *
     * @param archiveContext archiveContext
     */
    public static void printArchiveContext(Archive.Context archiveContext) {
        String aeronDirectoryName = archiveContext.aeronDirectoryName();
        String archiveDirectoryName =
                Paths.get(archiveContext.archiveDirectoryName()).resolve("").toAbsolutePath().toString();
        String localControlChannel = archiveContext.localControlChannel();
        String controlChannel = archiveContext.controlChannel();
        String recordingEventsChannel = archiveContext.recordingEventsChannel();
        String replicationChannel = archiveContext.replicationChannel();

        int localControlStreamId = archiveContext.localControlStreamId();
        int controlStreamId = archiveContext.controlStreamId();
        int recordingEventsStreamId = archiveContext.recordingEventsStreamId();
        boolean recordingEventsEnabled = archiveContext.recordingEventsEnabled();

        int controlMtuLength = archiveContext.controlMtuLength();
        int controlTermBufferLength = archiveContext.controlTermBufferLength();

        System.out.println(
                new StringJoiner("\n", "###  ArchiveContext:\n", "")
                        .add(String.join(": ", "aeronDirectoryName", aeronDirectoryName))
                        .add(String.join(": ", "archiveDirectoryName", archiveDirectoryName))
                        .add(String.join(": ", "controlChannel", controlChannel))
                        .add(String.join(": ", "controlStreamId", String.valueOf(controlStreamId)))
                        .add(String.join(": ", "localControlChannel", localControlChannel))
                        .add(String.join(": ", "localControlStreamId", String.valueOf(localControlStreamId)))
                        .add(String.join(": ", "recordingEventsEnabled", String.valueOf(recordingEventsEnabled)))
                        .add(String.join(": ", "recordingEventsChannel", recordingEventsChannel))
                        .add(String.join(": ", "recordingEventsStreamId", String.valueOf(recordingEventsStreamId)))
                        .add(String.join(": ", "replicationChannel", replicationChannel))
                        .add(String.join(": ", "controlMtuLength", String.valueOf(controlMtuLength)))
                        .add(String.join(": ", "controlTermBufferLength", String.valueOf(controlTermBufferLength))));
    }

    public static String controlRequestChannel(String endpoint) {
        return new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(endpoint).build();
    }

    public static String controlResponseChannel() {
        return new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint("localhost:0").build();
    }

    public static String localControlChannel(String instanceName) {
        return new ChannelUriStringBuilder().media(IPC_MEDIA).endpoint(instanceName).build();
    }

    /**
     * Prints publication.
     *
     * @param publication publication
     */
    public static void printPublication(Publication publication) {
        System.out.printf(
                "Publication, "
                + "channel: %s, "
                + "streamId: %d, "
                + "session: %d, "
                + "initialTermId: %d, "
                + "termLength: %d%n",
                publication.channel(),
                publication.streamId(),
                publication.sessionId(),
                publication.initialTermId(),
                publication.termBufferLength());
    }

    /**
     * Prints publication.
     *
     * @param publication publication
     */
    public static void printPublication(ExclusivePublication publication) {
        System.out.printf(
                "Publication, "
                + "channel: %s, "
                + "streamId: %d, "
                + "session: %d, "
                + "initialTermId: %d, "
                + "termId: %d, "
                + "termOffset: %d, "
                + "termLength: %d%n",
                publication.channel(),
                publication.streamId(),
                publication.sessionId(),
                publication.initialTermId(),
                publication.termId(),
                publication.termOffset(),
                publication.termBufferLength());
    }

    /**
     * Prints subscription info.
     *
     * @param subscription subscription
     */
    public static void printSubscription(final Subscription subscription) {
        System.out.printf(
                "Subscription %s, registrationId=%d, streamId=%d, imageCount=%d%n",
                subscription.channel(),
                subscription.registrationId(),
                subscription.streamId(),
                subscription.imageCount());
    }

    /**
     * Returns {@link FragmentHandler} instance which prints message to stdout.
     *
     * @param streamId streamId
     * @return result
     */
    public static FragmentHandler printAsciiMessage(final int streamId) {
        return (buffer, offset, length, header) ->
                System.out.printf(
                        "<<%s>> | "
                        + "session: %d, "
                        + "stream: %d, "
                        + "position: %d, "
                        + "initialTermId: %d, "
                        + "termId: %d, "
                        + "termOffset: %d%n",
                        buffer.getStringWithoutLengthAscii(offset, length),
                        header.sessionId(),
                        streamId,
                        header.position(),
                        header.initialTermId(),
                        header.termId(),
                        header.termOffset());
    }

    /**
     * Returns {@link FragmentHandler} instance which prints message to stdout.
     *
     * @param streamId streamId
     * @param label    label
     * @return result
     */
    public static FragmentHandler printAsciiMessage(final int streamId, String label) {
        return (buffer, offset, length, header) ->
                System.out.printf(
                        "<<%s>> '%s' | "
                        + "session: %d, "
                        + "stream: %d, "
                        + "position: %d, "
                        + "initialTermId: %d, "
                        + "termId: %d, "
                        + "termOffset: %d%n",
                        buffer.getStringWithoutLengthAscii(offset, length),
                        label,
                        header.sessionId(),
                        streamId,
                        header.position(),
                        header.initialTermId(),
                        header.termId(),
                        header.termOffset());
    }

    /**
     * Returns {@link FragmentHandler} instance which prints message to stdout.
     *
     * @param tps tps
     * @return result
     */
    public static FragmentHandler printAsciiMessage(ThroughputMeter tps) {
        return (buffer, offset, length, header) -> tps.record();
    }

    /**
     * Sends message and verifies result.
     *
     * @param publication publication
     * @param source      source
     * @param i           just i
     * @return result
     */
    public static long sendMessage(Publication publication, String source, long i) {
        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
        System.out.print("Offering " + i + ", source='" + source + "'" + "/" + NUMBER_OF_MESSAGES + " - ");
        final int length = buffer.putStringWithoutLengthAscii(0, "Hello World! " + i + ", source='" + source + "'");
        final long result = publication.offer(buffer, 0, length);
        verifyResult(publication, result);
        return result;
    }

    /**
     * Sends a message and verifies result.
     *
     * @param publication publication
     * @param i           just i
     */
    public static void sendMessage(Publication publication, long i) {
        System.out.println(new StringJoiner(", ", "yay! ", "")
                .add(String.join(": ", "sessionId", String.valueOf(publication.sessionId())))
                .add(String.join(": ", "streamId", String.valueOf(publication.streamId())))
                .add(String.join(": ", "position", String.valueOf(publication.position())))
                .add(String.join(": ", "numOfTermBuffers", String.valueOf(publication.position() / publication.termBufferLength())))
                .add(String.join(": ", "initialTermId", String.valueOf(publication.initialTermId())))
                .add(String.join(": ", "i", String.valueOf(i))));

        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
        System.out.print("Offering " + i + "/" + NUMBER_OF_MESSAGES + " - ");
        final int length = buffer.putStringWithoutLengthAscii(0, "Hello World! " + i);
        final long result = publication.offer(buffer, 0, length);
        verifyResult(publication, result);
    }

    /**
     * Sends a message and verifies result.
     *
     * @param publication publication
     * @param i           just i
     * @param tps         tps
     */
    public static void sendMessage(Publication publication, long i, ThroughputMeter tps) {
        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
        final int length = buffer.putStringWithoutLengthAscii(0, "Hello World! " + i);
        final long result = publication.offer(buffer, 0, length);
        verifyResult(publication, result, tps);
    }

    /**
     * Sends a message and verifies result.
     *
     * @param publication publication
     * @param i           just i
     */
    public static void sendMessageQuietly(Publication publication, long i) {
        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
        final int length = buffer.putStringWithoutLengthAscii(0, "Hello World! " + i);
        final long result = publication.offer(buffer, 0, length);
        verifyResultQuietly(publication, result);
    }

    /**
     * Sends a message and verifies result.
     *
     * @param counters    counters
     * @param publication publication
     * @param i           just i
     * @return result
     */
    public static long recordMessage(CountersReader counters, Publication publication, long i) {
        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
        System.out.print("Offering " + i + "/" + NUMBER_OF_MESSAGES + " - ");
        final int length = buffer.putStringWithoutLengthAscii(0, "Hello World! " + i);
        final long result = publication.offer(buffer, 0, length);
        verifyResult(counters, publication, result);
        return result;
    }

    static void verifyResult(CountersReader counters, Publication publication, long result) {
        if (result > 0) {
            int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
            long recordingId = RecordingPos.getRecordingId(counters, counterId);

            long recordingPos;
            int i = 0;
            long s = System.nanoTime();

            do {
                if (!RecordingPos.isActive(counters, counterId, recordingId)) {
                    throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
                }
                recordingPos = counters.getCounterValue(counterId);
                Thread.yield();
                i++;
            } while (recordingPos < publication.position());

            System.out.println(new StringJoiner(", ", "yay! ", "")
                    .add(String.join(": ", "sessionId", String.valueOf(publication.sessionId())))
                    .add(String.join(": ", "streamId", String.valueOf(publication.streamId())))
                    .add(String.join(": ", "position", String.valueOf(publication.position())))
                    .add(String.join(": ", "recordingPos", String.valueOf(recordingPos)))
                    .add(String.join(": ", "i", String.valueOf(i)))
                    .add(String.join(": ", "time", String.valueOf(System.nanoTime() - s))));
        } else if (result == Publication.BACK_PRESSURED) {
            System.out.println("Offer failed due to back pressure");
        } else if (result == Publication.NOT_CONNECTED) {
            System.out.println("Offer failed because publisher is not connected to a subscriber");
        } else if (result == Publication.ADMIN_ACTION) {
            System.out.println("Offer failed because of an administration action in the system");
        } else if (result == Publication.CLOSED) {
            System.out.println("Offer failed because publication is closed");
            throw new RuntimeException("Offer failed because publication is closed");
        } else if (result == Publication.MAX_POSITION_EXCEEDED) {
            System.out.println("Offer failed due to publication reaching its max position");
            throw new RuntimeException("Offer failed due to publication reaching its max position");
        } else {
            System.out.println("Offer failed due to unknown reason: " + result);
        }

        if (!publication.isConnected()) {
            System.out.println("No active subscribers detected");
        }
    }

    static void verifyResult(Publication publication, long result) {
        if (result > 0) {
            System.out.println(
                    new StringJoiner(", ", "yay! ", "")
                            .add(String.join(": ", "session", String.valueOf(publication.sessionId())))
                            .add(String.join(": ", "stream", String.valueOf(publication.streamId())))
                            .add(String.join(": ", "position", String.valueOf(publication.position())))
                            .add(String.join(": ", "positionLimit", String.valueOf(publication.positionLimit()))));
        } else if (result == Publication.BACK_PRESSURED) {
            System.out.println("Offer failed due to back pressure");
        } else if (result == Publication.NOT_CONNECTED) {
            System.out.println("Offer failed because publisher is not connected to a subscriber");
        } else if (result == Publication.ADMIN_ACTION) {
            System.out.println("Offer failed because of an administration action in the system");
        } else if (result == Publication.CLOSED) {
            System.out.println("Offer failed because publication is closed");
            throw new RuntimeException("Offer failed because publication is closed");
        } else if (result == Publication.MAX_POSITION_EXCEEDED) {
            System.out.println("Offer failed due to publication reaching its max position");
            throw new RuntimeException("Offer failed due to publication reaching its max position");
        } else {
            System.out.println("Offer failed due to unknown reason: " + result);
        }

        if (!publication.isConnected()) {
            System.out.println("No active subscribers detected");
        }
    }

    //@SuppressWarnings("StatementWithEmptyBody")
    static void verifyResultQuietly(Publication publication, long result) {
        if (result > 0) {
            // no-op
        } else if (result == Publication.BACK_PRESSURED) {
            System.out.printf(
                    "Offer failed due to back pressure (position: %d)%n", publication.position());
        } else if (result == Publication.NOT_CONNECTED) {
            System.out.println("Offer failed because publisher is not connected to a subscriber");
        } else if (result == Publication.ADMIN_ACTION) {
            System.out.printf(
                    "Offer failed because of an administration action in the system (position: %d)%n",
                    publication.position());
        } else if (result == Publication.CLOSED) {
            System.out.printf(
                    "Offer failed because publication is closed (position: %d)%n", publication.position());
            throw new RuntimeException("Offer failed because publication is closed");
        } else if (result == Publication.MAX_POSITION_EXCEEDED) {
            System.out.printf(
                    "Offer failed due to publication reaching its max position (position: %d)%n",
                    publication.position());
            throw new RuntimeException("Offer failed due to publication reaching its max position");
        } else {
        }

        if (!publication.isConnected()) {
        }
    }

    //@SuppressWarnings("StatementWithEmptyBody")
    static void verifyResult(Publication publication, long result, ThroughputMeter tps) {
        if (result > 0) {
            tps.record();
        } else if (result == Publication.BACK_PRESSURED) {
            // no-op
        } else if (result == Publication.NOT_CONNECTED) {
            // no-op
        } else if (result == Publication.ADMIN_ACTION) {
            // no-op
        } else if (result == Publication.CLOSED) {
            throw new RuntimeException("Offer failed because publication is closed");
        } else if (result == Publication.MAX_POSITION_EXCEEDED) {
            throw new RuntimeException("Offer failed due to publication reaching its max position");
        } else {
            throw new RuntimeException("Offer failed due to unknown reason: " + result);
        }
    }
}
