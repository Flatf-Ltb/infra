package io.scalecube.aeron.examples;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.NUMBER_OF_MESSAGES;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printPublication;

public class MultiEndpointSender {

    public static final String ENDPOINT = "localhost:20121";
    public static final String ENDPOINT2 = "localhost:20122";
    public static final String ENDPOINT3 = "localhost:20123";

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
        //SigInt.register(MultiEndpointSender::close);

        mediaDriver = MediaDriver.launchEmbedded();
        String aeronDirectoryName = mediaDriver.aeronDirectoryName();

        Context context = new Context()
                .aeronDirectoryName(aeronDirectoryName)
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage);

        aeron = Aeron.connect(context);
        System.out.println("hello, " + context.aeronDirectoryName());

        String channel = new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(ENDPOINT).build();
        String channel2 = new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(ENDPOINT2).build();
        String channel3 = new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(ENDPOINT3).build();

        ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID);
        ExclusivePublication publication2 = aeron.addExclusivePublication(channel2, STREAM_ID);
        ExclusivePublication publication3 = aeron.addExclusivePublication(channel3, STREAM_ID);

        printPublication(publication);
        printPublication(publication2);
        printPublication(publication3);

        for (long i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AeronHelper.sendMessage(publication, i);
            AeronHelper.sendMessage(publication2, i);
            AeronHelper.sendMessage(publication3, i);
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
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
