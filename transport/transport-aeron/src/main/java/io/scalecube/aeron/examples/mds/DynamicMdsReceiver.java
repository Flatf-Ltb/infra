package io.scalecube.aeron.examples.mds;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.scalecube.aeron.examples.AeronHelper;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printAsciiMessage;

public class DynamicMdsReceiver {

    public static final String CONTROL_ENDPOINT = "localhost:30121";
    public static final String CONTROL_ENDPOINT2 = "localhost:30122";

    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static MediaDriver mediaDriver;
    private static Aeron aeron;

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) {
//        SigInt.register(DynamicMdsReceiver::close);

        mediaDriver = MediaDriver.launchEmbedded();
        String aeronDirectoryName = mediaDriver.aeronDirectoryName();

        Context context = new Context()
                .aeronDirectoryName(aeronDirectoryName)
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage);

        aeron = Aeron.connect(context);
        System.out.println("hello, " + context.aeronDirectoryName());

        String controlChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA).controlMode(MDC_CONTROL_MODE_MANUAL).build();

        Subscription subscription = aeron.addSubscription(controlChannel, STREAM_ID);

        String destinationChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint("localhost:0")
                .controlEndpoint(CONTROL_ENDPOINT)
                .build();
        String destinationChannel2 = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint("localhost:0")
                .controlEndpoint(CONTROL_ENDPOINT2)
                .build();

        subscription.addDestination(destinationChannel);
        subscription.addDestination(destinationChannel2);

        AeronHelper.printSubscription(subscription);

        final FragmentHandler fragmentHandler = printAsciiMessage(STREAM_ID);
        FragmentAssembler fragmentAssembler = new FragmentAssembler(fragmentHandler);
        BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();

        while (running.get()) {
            idleStrategy.idle(subscription.poll(fragmentAssembler, FRAGMENT_LIMIT));
        }

        System.out.println("Shutting down...");

        close();
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
