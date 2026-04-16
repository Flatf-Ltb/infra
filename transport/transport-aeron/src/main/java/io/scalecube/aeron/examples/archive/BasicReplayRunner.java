package io.scalecube.aeron.examples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.logbuffer.FragmentHandler;
import io.scalecube.aeron.examples.AeronHelper;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printAsciiMessage;

public class BasicReplayRunner {

    public static final String CHANNEL = "aeron:udp?endpoint=localhost:20121";

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
//        SigInt.register(BasicReplayRunner::close);

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
        System.out.printf("### found rd: %s%n", rd);

        Subscription subscription = aeronArchive
                .replay(rd.recordingId(), 0, Long.MAX_VALUE, CHANNEL, STREAM_ID);
        Subscription subscription2 = aeronArchive
                .replay(rd.recordingId(), 0, Long.MAX_VALUE, CHANNEL, STREAM_ID);
        Subscription subscription3 = aeronArchive
                .replay(rd.recordingId(), 0, Long.MAX_VALUE, CHANNEL, STREAM_ID);

        final FragmentHandler fragmentHandler = printAsciiMessage(STREAM_ID);
        FragmentAssembler fragmentAssembler = new FragmentAssembler(fragmentHandler);
        FragmentAssembler fragmentAssembler2 = new FragmentAssembler(fragmentHandler);
        FragmentAssembler fragmentAssembler3 = new FragmentAssembler(fragmentHandler);
        BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();

        while (running.get()) {
            idleStrategy.idle(subscription.poll(fragmentAssembler, FRAGMENT_LIMIT));
            idleStrategy.idle(subscription2.poll(fragmentAssembler2, FRAGMENT_LIMIT));
            idleStrategy.idle(subscription3.poll(fragmentAssembler3, FRAGMENT_LIMIT));
        }

        System.out.println("Shutting down...");

        close();
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
