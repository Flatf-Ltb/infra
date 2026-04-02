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
import static io.scalecube.aeron.examples.AeronHelper.NUMBER_OF_MESSAGES;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printPublication;

public class SingleSessionMdsSender {

    public static final String REPLAY_ENDPOINT = "localhost:20121";
    public static final String CONTROL_ENDPOINT = "localhost:30121";
    public static final int SESSION_ID = 100500;
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

//        SigInt.register(SingleSessionMdsSender::close);

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
                .controlEndpoint(CONTROL_ENDPOINT)
                .sessionId(SESSION_ID)
                .build();

        ExclusivePublication recordingPublication = aeron
                .addExclusivePublication(recordingChannel, STREAM_ID);
        printPublication(recordingPublication);

        // spy recording (need it very much to keep updating sender position counter in network
        // publication, see io.aeron.driver.NetworkPublication.send)
        Subscription spyRecordingSubscription = aeron
                .addSubscription(String.join(":", SPY_QUALIFIER, recordingChannel), STREAM_ID);

        // replay
        String replayChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .initialPosition(0L, recordingPublication.initialTermId(),
                        recordingPublication.termBufferLength())
                .endpoint(REPLAY_ENDPOINT)
                .sessionId(SESSION_ID)
                .linger(0L)
                .eos(false)
                .build();

        ExclusivePublication replayPublication = aeron
                .addExclusivePublication(replayChannel, STREAM_ID);
        printPublication(replayPublication);

        // spy replay (left here for research purpose, somehow presence of this spy prohibits from
        // getting traffik on live destination, because !isFlowControlOverRun/!isFlowControlUnderRun
        // conditions can't be met, somehow this "replay spy" creates situation when live channel is at
        // underRun condition (i.e. !isFlowControlUnderRun(packetPosition) -> false), appears is , see
        // io.aeron.driver.PublicationImage.insertPacket)
        Subscription spyReplaySubscription = aeron
                .addSubscription(String.join(":", SPY_QUALIFIER, replayChannel), STREAM_ID);

        // send
        for (long i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AeronHelper.sendMessage(replayPublication, "replay", i); // replay(position)
            AeronHelper.sendMessage(recordingPublication, "live", i); // market service events
            spyRecordingSubscription.poll(AeronHelper.NOOP_FRAGMENT_HANDLER, 10); // poll spy
            spyReplaySubscription.poll(AeronHelper.NOOP_FRAGMENT_HANDLER, 10); // poll spy
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
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
