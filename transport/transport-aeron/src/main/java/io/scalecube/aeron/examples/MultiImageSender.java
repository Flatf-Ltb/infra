package io.scalecube.aeron.examples;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printPublication;

public class MultiImageSender {

    public static final String ENDPOINT = "localhost:20121";
    public static final int TERM_LENGTH = 65536;

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
        //SigInt.register(MultiImageSender::close);

        mediaDriver = MediaDriver.launchEmbedded();
        String aeronDirectoryName = mediaDriver.aeronDirectoryName();

        Context context = new Context()
                .aeronDirectoryName(aeronDirectoryName)
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage);

        aeron = Aeron.connect(context);
        System.out.println("hello, " + context.aeronDirectoryName());

        final String channel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .termLength(TERM_LENGTH)
                .endpoint(ENDPOINT)
                .build();

        ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID);

        printPublication(publication);

        final IdleStrategy idleStrategy = new SleepingIdleStrategy(1);

        int i = 0;

        while (running.get()) {
            AeronHelper.sendMessageQuietly(publication, i++);
            idleStrategy.idle();
        }

        System.out.println("Done sending");

        close();
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
