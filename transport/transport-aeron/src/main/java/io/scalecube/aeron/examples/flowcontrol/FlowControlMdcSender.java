package io.scalecube.aeron.examples.flowcontrol;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.driver.MediaDriver;
import io.scalecube.aeron.examples.AeronHelper;
import io.scalecube.aeron.examples.meter.MeterRegistry;
import io.scalecube.aeron.examples.meter.ThroughputMeter;
import org.agrona.CloseHelper;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.STREAM_ID;
import static io.scalecube.aeron.examples.AeronHelper.printPublication;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FlowControlMdcSender {

    public static final String CONTROL_ENDPOINT = "localhost:30121";

    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

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
//        SigInt.register(() -> running.set(false));

        try {
            final Integer messageRate = Integer.getInteger("messageRate");

            if (messageRate == null)
                throw new IllegalArgumentException("messageRate must not be null");

            System.out.printf("### messageRate: %s%n", messageRate);

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
                    .build();

            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID);

            printPublication(publication);

            meterRegistry = MeterRegistry.create();
            final ThroughputMeter tps = meterRegistry.tps("sender.tps");

            long sendInterval = NANOS_PER_SECOND / (messageRate * (long) 1e3);
            long sendIntervalDeadline = -1;

            for (long i = 0; running.get(); i++) {
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

    private static void close() {
        CloseHelper.close(meterRegistry);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
