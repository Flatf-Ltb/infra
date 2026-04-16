package io.scalecube.aeron.examples.flowcontrol;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.scalecube.aeron.examples.AeronHelper;
import io.scalecube.aeron.examples.meter.MeterRegistry;
import io.scalecube.aeron.examples.meter.ThroughputMeter;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printAsciiMessage;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;

public class FlowControlMdcReceiver {

    public static final String CONTROL_ENDPOINT = "localhost:30121";

    private static final Integer pollDelayMillis = Integer.getInteger("pollDelayMillis");
    private static final String receiverCategory = System.getProperty("receiverCategory");

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static MeterRegistry meterRegistry;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
        // SigInt.register(() -> running.set(false));

        try {
            if (receiverCategory == null)
                throw new IllegalArgumentException("receiverCategory must not be null");

            System.out.printf("### receiverCategory: %s, pollDelayMillis: %s%n",
                    receiverCategory.replace("\\s+", ""), pollDelayMillis);

            mediaDriver = MediaDriver.launchEmbedded();
            String aeronDirectoryName = mediaDriver.aeronDirectoryName();

            Context context = new Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .availableImageHandler(AeronHelper::printAvailableImage)
                    .unavailableImageHandler(AeronHelper::printUnavailableImage);

            aeron = Aeron.connect(context);
            System.out.println("hello, " + context.aeronDirectoryName());

            String channel = new ChannelUriStringBuilder()
                    .media(UDP_MEDIA)
                    .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                    .controlEndpoint(CONTROL_ENDPOINT)
                    .endpoint("localhost:0")
                    .build();

            Subscription subscription = aeron
                    .addSubscription(channel, STREAM_ID); // conn: 20121 / logbuffer: 48M

            printSubscription(subscription);

            final Image image = awaitImage(subscription);

            meterRegistry = MeterRegistry.create();
            final ThroughputMeter tps = meterRegistry
                    .tps(receiverCategory.replace("\\s+", "") + ".receiver.tps");

            final FragmentAssembler fragmentAssembler = new FragmentAssembler(printAsciiMessage(tps));

            while (running.get()) {
                pollImageUntilClosed(fragmentAssembler, image, FRAGMENT_LIMIT);
                if (pollDelayMillis != null && pollDelayMillis > 0) {
                    TimeUnit.MILLISECONDS.sleep(pollDelayMillis);
                }
            }
        } catch (Throwable th) {
            close();
            throw th;
        }

        System.out.println("Shutting down...");

        close();
    }

    private static Image awaitImage(Subscription subscription) {
        final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();
        while (subscription.imageCount() == 0) {
            idleStrategy.idle();
            if (!running.get()) {
                throw new RuntimeException("Not running anymore");
            }
        }
        return subscription.imageAtIndex(0);
    }

    private static void pollImageUntilClosed(
            FragmentAssembler fragmentAssembler, Image image, int fragmentLimit) {
        if (image.isClosed()) {
            throw new RuntimeException("Image is closed, image: " + image);
        } else {
            image.poll(fragmentAssembler, fragmentLimit);
        }
    }

    private static void close() {
        CloseHelper.close(meterRegistry);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
