package io.scalecube.aeron.examples;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;

public class MultiEndpointReceiver {

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
    public static void main(String[] args) {
        //SigInt.register(MultiEndpointReceiver::close);

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

        Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
        Subscription subscription2 = aeron.addSubscription(channel2, STREAM_ID);
        Subscription subscription3 = aeron.addSubscription(channel3, STREAM_ID);

        printSubscription(subscription);
        printSubscription(subscription2);
        printSubscription(subscription3);

        final FragmentHandler fragmentHandler = AeronHelper.printAsciiMessage(STREAM_ID);
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
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
