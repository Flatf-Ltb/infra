package io.scalecube.aeron.examples.mdc;

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

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printAsciiMessage;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;

public class DynamicMdcReceiver {

    public static final String CONTROL_ENDPOINT = "localhost:30121";

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) {
        // SigInt.register(DynamicMdcReceiver::close);

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

        Subscription subscriptionFoo = aeron
                // conn: 20121 / logbuffer: 48M
                .addSubscription(channel, STREAM_ID);
        Subscription subscriptionBar = aeron
                // conn: 20121 / logbuffer: 48M
                .addSubscription(channel, STREAM_ID);

        printSubscription(subscriptionFoo);
        printSubscription(subscriptionBar);

        final FragmentHandler fragmentHandler = printAsciiMessage(STREAM_ID);
        FragmentAssembler fragmentAssemblerFoo = new FragmentAssembler(fragmentHandler);
        FragmentAssembler fragmentAssemblerBar = new FragmentAssembler(fragmentHandler);
        BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();

        while (running.get()) {
            idleStrategy.idle(subscriptionFoo.poll(fragmentAssemblerFoo, FRAGMENT_LIMIT));
            idleStrategy.idle(subscriptionBar.poll(fragmentAssemblerBar, FRAGMENT_LIMIT));
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
