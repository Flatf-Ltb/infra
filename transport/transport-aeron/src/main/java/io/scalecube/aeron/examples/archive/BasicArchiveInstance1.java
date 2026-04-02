package io.scalecube.aeron.examples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.scalecube.aeron.examples.AeronHelper;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;

import java.nio.file.Path;
import java.nio.file.Paths;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.scalecube.aeron.examples.AeronHelper.localControlChannel;
import static io.scalecube.aeron.examples.AeronHelper.printArchiveContext;

public class BasicArchiveInstance1 {

    public static final String RECORDING_EVENTS_CHANNEL_ENDPOINT = "localhost:8030";
    public static final String CONTROL_CHANNEL_ENDPOINT = "localhost:8010";
    public static final String REPLICATION_CHANNEL_ENDPOINT = "localhost:0";

    private static MediaDriver mediaDriver;
    private static Aeron aeron;
    private static Archive archive;

    /**
     * Main runner.
     *
     * @param args args
     */
    public static void main(String[] args) throws InterruptedException {
        // SigInt.register(BasicArchiveInstance1::close);

        Path aeronPath = Paths.get(CommonContext.generateRandomDirName());
        String instanceName = aeronPath.getFileName().toString();
        Path archivePath = AeronHelper.archivePath()
                .orElseGet(() -> Paths.get(String.join("-", instanceName, "archive")));

        mediaDriver = MediaDriver.launch(
                new Context().aeronDirectoryName(aeronPath.toString()).spiesSimulateConnection(true));

        aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(aeronPath.toString())
                .availableImageHandler(AeronHelper::printAvailableImage)
                .unavailableImageHandler(AeronHelper::printUnavailableImage));

        archive = Archive.launch(new Archive.Context()
                .aeron(aeron)
                .mediaDriverAgentInvoker(mediaDriver.sharedAgentInvoker())
                .errorCounter(new AtomicCounter(
                        mediaDriver.context().countersValuesBuffer(),
                        SystemCounterDescriptor.ERRORS.id()))
                .errorHandler(mediaDriver.context().errorHandler())
                .localControlChannel(localControlChannel(instanceName))
                .controlChannel(controlChannel())
                .recordingEventsChannel(recordingEventsChannel())
                .replicationChannel(replicationChannel())
                .aeronDirectoryName(aeronPath.toString())
                .archiveDirectoryName(archivePath.toString())
                .threadingMode(ArchiveThreadingMode.SHARED));

        printArchiveContext(archive.context());

        System.out.println("Exiting ...");

        close();
    }

    private static String replicationChannel() {
        return new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint(REPLICATION_CHANNEL_ENDPOINT)
                .build();
    }

    private static String recordingEventsChannel() {
        return new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .controlMode(MDC_CONTROL_MODE_DYNAMIC)
                .controlEndpoint(RECORDING_EVENTS_CHANNEL_ENDPOINT)
                .build();
    }

    private static String controlChannel() {
        return new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint(CONTROL_CHANNEL_ENDPOINT)
                .build();
    }

    private static void close() {
        CloseHelper.close(archive);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }
}
