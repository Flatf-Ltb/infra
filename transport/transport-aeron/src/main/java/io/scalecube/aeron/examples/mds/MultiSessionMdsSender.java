package io.scalecube.aeron.examples.mds;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.agent.EventLogAgent;
import io.aeron.driver.MediaDriver;
import io.scalecube.aeron.examples.AeronHelper;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.CloseHelper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.NOOP_FRAGMENT_HANDLER;
import static io.scalecube.aeron.examples.AeronHelper.NUMBER_OF_MESSAGES;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printPublication;

public class MultiSessionMdsSender {

    public static final String RECORDING_ENDPOINT = "localhost:30121";
    public static final String REPLAY_BOB_ENDPOINT = "localhost:20121";
    public static final String REPLAY_ALICE_ENDPOINT = "localhost:20122";

    public static final int RECORDING_SESSION_ID = 100500;
    public static final int REPLAY_BOB_SESSION_ID = 100600;
    public static final int REPLAY_ALICE_SESSION_ID = 100700;

    public static final long PUBLICATION_LINGER_TIMEOUT = TimeUnit.SECONDS.toNanos(3);
    public static final long IMAGE_LIVENESS_TIMEOUT = TimeUnit.SECONDS.toNanos(3);

    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static MediaDriver mediaDriver;
    private static Aeron aeron;

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("aeron.event.log", "admin");
        System.setProperty("aeron.event.archive.log", "all");
        EventLogAgent.agentmain("", ByteBuddyAgent.install());

//        SigInt.register(MultiSessionMdsSender::close);

        mediaDriver = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                        .spiesSimulateConnection(true)
                        .publicationLingerTimeoutNs(PUBLICATION_LINGER_TIMEOUT)
                        .imageLivenessTimeoutNs(IMAGE_LIVENESS_TIMEOUT));

        String aeronDirectoryName = mediaDriver.aeronDirectoryName();

        Context context = new Context()
                .aeronDirectoryName(aeronDirectoryName)
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage);

        aeron = Aeron.connect(context);
        System.out.println("hello, " + context.aeronDirectoryName());

        // recording
        String recordingChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                .controlEndpoint(RECORDING_ENDPOINT)
                .sessionId(RECORDING_SESSION_ID)
                .build();

        ExclusivePublication recordingPublication = aeron
                .addExclusivePublication(recordingChannel, STREAM_ID);
        printPublication(recordingPublication);

        // spy recording
        Subscription spyRecordingSubscription = aeron
                .addSubscription(String.join(":", SPY_QUALIFIER, recordingChannel), STREAM_ID);

        // replay 1
        String replayChannelBob = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .initialPosition(0, recordingPublication.initialTermId(),
                        recordingPublication.termBufferLength())
                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                .controlEndpoint(REPLAY_BOB_ENDPOINT)
                .sessionId(REPLAY_BOB_SESSION_ID)
                .linger(0L)
                .eos(false)
                .build();

        ExclusivePublication replayPublicationBob = aeron
                .addExclusivePublication(replayChannelBob, STREAM_ID);
        printPublication(replayPublicationBob);

        // replay 2
        String replayChannelAlice = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .initialPosition(0, recordingPublication.initialTermId(),
                        recordingPublication.termBufferLength())
                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                .controlEndpoint(REPLAY_ALICE_ENDPOINT)
                .sessionId(REPLAY_ALICE_SESSION_ID)
                .linger(0L)
                .eos(false)
                .build();

        ExclusivePublication replayPublicationAlice = aeron
                .addExclusivePublication(replayChannelAlice, STREAM_ID);
        printPublication(replayPublicationAlice);

        // send
        for (long i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AeronHelper.sendMessage(replayPublicationBob, "bob:replay", i); // replay(position)
            AeronHelper.sendMessage(replayPublicationAlice, "alice:replay", i); // replay(position)
            AeronHelper.sendMessage(recordingPublication, "live", i); // market service events
            spyRecordingSubscription.poll(NOOP_FRAGMENT_HANDLER, 10); // poll spy
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(300));
        }

        System.out.println("Done sending");

        close();
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(mediaDriver);
        CloseHelper.close(aeron);
    }
}
