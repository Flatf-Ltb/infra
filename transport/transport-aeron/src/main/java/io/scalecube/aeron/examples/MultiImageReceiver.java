package io.scalecube.aeron.examples;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;

public class MultiImageReceiver {

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
    public static void main(String[] args) {
        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(MultiImageReceiver::close);) {


            mediaDriver = MediaDriver.launchEmbedded(
                    new MediaDriver.Context().imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3)));
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

            // yes, several subscriptions on the same channel
            Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Subscription subscription2 = aeron.addSubscription(channel, STREAM_ID);
            Subscription subscription3 = aeron.addSubscription(channel, STREAM_ID);

            printSubscription(subscription);
            printSubscription(subscription2);
            printSubscription(subscription3);

            awaitImage(subscription);
            awaitImage(subscription2);
            awaitImage(subscription3);

            FragmentAssembler fragmentAssembler = new FragmentAssembler(AeronHelper
                    .printAsciiMessage(STREAM_ID, "1"));
            FragmentAssembler fragmentAssembler2 = new FragmentAssembler(AeronHelper
                    .printAsciiMessage(STREAM_ID, "2"));
            FragmentAssembler fragmentAssembler3 = new FragmentAssembler(AeronHelper
                    .printAsciiMessage(STREAM_ID, "3"));

            final Image image = subscription.imageAtIndex(0);
            final Image image2 = subscription2.imageAtIndex(0);
            final Image image3 = subscription3.imageAtIndex(0);

            long time = System.nanoTime();
            long step = TimeUnit.SECONDS.toNanos(1);
            long next = time + step;

            while (running.get()) {
                final long now = System.nanoTime();

                pollImageIfNotClosed(fragmentAssembler, image, FRAGMENT_LIMIT);

                if (now >= next) {
                    next = now + step;
                    pollImageIfNotClosed(fragmentAssembler2, image2, 1);
                    pollImageIfNotClosed(fragmentAssembler3, image3, 1);
                }
            }

            System.out.println("Shutting down...");

            close();
            barrier.signal();
        }
    }

    private static void awaitImage(Subscription subscription) {
        final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();
        while (subscription.imageCount() == 0) {
            idleStrategy.idle();
        }
    }

    private static void pollImageIfNotClosed(
            FragmentAssembler fragmentAssembler, Image image, int fragmentLimit) {
        if (image.isClosed()) {
            throw new RuntimeException("Image is closed, image: " + image);
        } else {
            image.poll(fragmentAssembler, fragmentLimit);
        }
    }

    private static void close() {
        running.set(false);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }

}
