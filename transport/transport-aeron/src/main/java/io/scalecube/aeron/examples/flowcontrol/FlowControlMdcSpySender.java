package io.scalecube.aeron.examples.flowcontrol;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.scalecube.aeron.examples.AeronHelper;
import io.scalecube.aeron.examples.meter.MeterRegistry;
import io.scalecube.aeron.examples.meter.ThroughputMeter;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.FRAGMENT_LIMIT;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printAsciiMessage;
import static io.scalecube.aeron.examples.AeronHelper.printPublication;
import static io.scalecube.aeron.examples.AeronHelper.printSubscription;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FlowControlMdcSpySender {

    public static final String CONTROL_ENDPOINT = "localhost:30121";

    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

    private static final Integer pollSpyDelayMillis = Integer.getInteger("pollSpyDelayMillis");
    private static final Integer messageRate = Integer.getInteger("messageRate");

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static MeterRegistry meterRegistry;
    private static AgentRunner agentRunner;

    private static final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
//        SigInt.register(() -> running.set(false));

        try {
            if (messageRate == null)
                throw new IllegalArgumentException("messageRate must not be null");

            System.out.printf("### messageRate: %s, pollSpyDelayMillis: %s%n",
                    messageRate, pollSpyDelayMillis);

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
                    .spiesSimulateConnection(true)
                    .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                    .controlEndpoint(CONTROL_ENDPOINT)
                    .build();

            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID);

            printPublication(publication);

            meterRegistry = MeterRegistry.create();
            final ThroughputMeter tps = meterRegistry.tps("sender.tps");

            long sendInterval = NANOS_PER_SECOND / (messageRate * (long) 1e3);
            long sendIntervalDeadline = -1;

            final SpyAgent spyAgent = new SpyAgent();
            agentRunner = new AgentRunner(NoOpIdleStrategy.INSTANCE, throwable -> {
            }, null, spyAgent);
            AgentRunner.startOnThread(agentRunner);

            for (long i = 0; running.get() && !spyAgent.closed; i++) {
                long now = System.nanoTime();
                if (now >= sendIntervalDeadline) {
                    AeronHelper.sendMessage(publication, i, tps);
                    sendIntervalDeadline = now + sendInterval;
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

    private static int pollImageUntilClosed(FragmentAssembler fragmentAssembler,
                                            Image image, int fragmentLimit) {
        if (image.isClosed()) {
            throw new RuntimeException("Image is closed, image: " + image);
        } else {
            return image.poll(fragmentAssembler, fragmentLimit);
        }
    }

    private static void close() {
        CloseHelper.close(agentRunner);
        CloseHelper.close(meterRegistry);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }

    private static class SpyAgent implements Agent {

        private Image image;
        private FragmentAssembler fragmentAssembler;

        private volatile boolean closed;

        @Override
        public void onStart() {
            String channel = new ChannelUriStringBuilder()
                    .media(UDP_MEDIA)
                    .prefix(SPY_QUALIFIER)
                    .controlEndpoint(CONTROL_ENDPOINT)
                    .build();

            final Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            printSubscription(subscription);

            image = awaitImage(subscription);

            ThroughputMeter tps = meterRegistry.tps("spy.receiver.tps");
            fragmentAssembler = new FragmentAssembler(printAsciiMessage(tps));
        }

        @Override
        public int doWork() {
            try {
                final int work = pollImageUntilClosed(fragmentAssembler, image, FRAGMENT_LIMIT);
                if (pollSpyDelayMillis != null && pollSpyDelayMillis > 0) {
                    TimeUnit.MILLISECONDS.sleep(pollSpyDelayMillis);
                }
                return work;
            } catch (Throwable th) {
                System.err.println("[spy-agent][error] cause: " + th);
                throw new AgentTerminationException(th);
            }
        }

        @Override
        public void onClose() {
            closed = true;
        }

        @Override
        public String roleName() {
            return "spy-agent";
        }
    }

}
