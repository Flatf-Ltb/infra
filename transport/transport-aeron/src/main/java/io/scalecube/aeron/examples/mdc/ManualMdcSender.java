package io.scalecube.aeron.examples.mdc;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.driver.MediaDriver;
import io.scalecube.aeron.examples.AeronHelper;
import org.agrona.CloseHelper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.NUMBER_OF_MESSAGES;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printPublication;

public class ManualMdcSender {

    public static final String ENDPOINT = "localhost:20121";
    public static final String ENDPOINT2 = "localhost:20122";

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
//        SigInt.register(ManualMdcSender::close);

        mediaDriver = MediaDriver.launchEmbedded();
        String aeronDirectoryName = mediaDriver.aeronDirectoryName();

        Context context = new Context()
                .aeronDirectoryName(aeronDirectoryName)
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage);

        aeron = Aeron.connect(context);
        System.out.println("hello, " + context.aeronDirectoryName());

        String controlEndpointChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA).controlMode(MDC_CONTROL_MODE_MANUAL).build();

        ExclusivePublication publication =
                aeron.addExclusivePublication(controlEndpointChannel, STREAM_ID);

        String destinationChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA).endpoint(ENDPOINT).build();
        String destinationChannel2 = new ChannelUriStringBuilder()
                .media(UDP_MEDIA).endpoint(ENDPOINT2).build();

        publication.addDestination(destinationChannel);
        publication.addDestination(destinationChannel2);

        printPublication(publication);

        for (long i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AeronHelper.sendMessage(publication, i);
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
