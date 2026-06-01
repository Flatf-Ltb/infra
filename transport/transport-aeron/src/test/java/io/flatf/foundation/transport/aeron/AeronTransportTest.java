package io.flatf.foundation.transport.aeron;

import io.flatf.foundation.transport.api.Transport;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AeronTransportTest {

    @Test
    void ipcConfigRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> AeronConfig.ipc(0));
        assertThrows(IllegalArgumentException.class, () -> AeronConfig.udp("127.0.0.1", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronConfig.udp("127.0.0.1", 13000, -1));
    }

    @Test
    void publisherRejectsMismatchedTargetStream() {
        AeronConfig cfg = AeronConfig.ipc(31001).withPublishRetryCount(1);
        try (AeronPublisher<String> publisher = cfg.createPublisher(msg -> msg.getBytes(StandardCharsets.UTF_8))) {
            assertFalse(publisher.supportsTargetedPublish());
            assertThrows(IllegalArgumentException.class, () -> publisher.publish(31002, "payload"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void ipcPublishSubscribeRoundTrip() throws Exception {
        AeronConfig cfg = AeronConfig.ipc(31011).withPublishRetryCount(200);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> payloadRef = new AtomicReference<>();
        AtomicReference<Integer> streamRef = new AtomicReference<>();

        try (AeronSubscriber subscriber = cfg.createSubscriber(new int[]{31011}, (streamId, payload) -> {
            streamRef.set(streamId);
            payloadRef.set(new String(payload, StandardCharsets.UTF_8));
            latch.countDown();
        });
             AeronPublisher<String> publisher = cfg.createPublisher(msg -> msg.getBytes(StandardCharsets.UTF_8))) {

            Thread subscriberThread = new Thread(subscriber, "aeron-subscriber-test");
            subscriberThread.start();

            waitUntilConnected(subscriber, publisher);
            publisher.publish("hello-aeron");

            assertTrue(latch.await(5, TimeUnit.SECONDS), "subscriber should receive the published payload");
            assertEquals(Integer.valueOf(31011), streamRef.get());
            assertEquals("hello-aeron", payloadRef.get());

            subscriber.closeIgnoreException();
            subscriberThread.join(TimeUnit.SECONDS.toMillis(3));
            assertFalse(subscriberThread.isAlive(), "subscriber thread should stop after close");
        }
    }

    @Test
    void ipcZeroCopyPublishSubscribeRoundTrip() throws Exception {
        AeronConfig cfg = AeronConfig.ipc(31021).withPublishRetryCount(200);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger messageIndex = new AtomicInteger();
        AtomicReference<String> firstPayload = new AtomicReference<>();
        AtomicReference<String> secondPayload = new AtomicReference<>();
        AtomicReference<Integer> firstStream = new AtomicReference<>();
        AtomicReference<Integer> secondStream = new AtomicReference<>();
        AtomicReference<Integer> firstViewIdentity = new AtomicReference<>();
        AtomicReference<Integer> secondViewIdentity = new AtomicReference<>();

        try (AeronZeroCopySubscriber subscriber = cfg.createZeroCopySubscriber(new int[]{31021}, (streamId, view) -> {
            int current = messageIndex.getAndIncrement();
            if (current == 0) {
                firstStream.set(streamId);
                firstPayload.set(new String(view.copyBytes(), StandardCharsets.UTF_8));
                firstViewIdentity.set(System.identityHashCode(view));
            } else if (current == 1) {
                secondStream.set(streamId);
                secondPayload.set(new String(view.copyBytes(), StandardCharsets.UTF_8));
                secondViewIdentity.set(System.identityHashCode(view));
            }
            latch.countDown();
        });
             AeronPublisher<String> publisher = cfg.createPublisher(msg -> msg.getBytes(StandardCharsets.UTF_8))) {

            Thread subscriberThread = new Thread(subscriber, "aeron-zero-copy-subscriber-test");
            subscriberThread.start();

            waitUntilConnected(subscriber, publisher);
            publisher.publish("hello");
            publisher.publish("world");

            assertTrue(latch.await(5, TimeUnit.SECONDS), "subscriber should receive both zero-copy payloads");
            assertEquals(Integer.valueOf(31021), firstStream.get());
            assertEquals(Integer.valueOf(31021), secondStream.get());
            assertEquals("hello", firstPayload.get());
            assertEquals("world", secondPayload.get());
            assertEquals(firstViewIdentity.get(), secondViewIdentity.get(), "view wrapper should be reused");

            subscriber.closeIgnoreException();
            subscriberThread.join(TimeUnit.SECONDS.toMillis(3));
            assertFalse(subscriberThread.isAlive(), "zero-copy subscriber thread should stop after close");
        }
    }

    @Test
    void publicationResultHelpersClassifyFailures() {
        assertTrue(AeronPublisher.isRetryable(io.aeron.Publication.NOT_CONNECTED));
        assertTrue(AeronPublisher.isRetryable(io.aeron.Publication.ADMIN_ACTION));
        assertFalse(AeronPublisher.isRetryable(io.aeron.Publication.CLOSED));
        assertEquals("BACK_PRESSURED", AeronPublisher.describeOfferResult(io.aeron.Publication.BACK_PRESSURED));
    }

    private static void waitUntilConnected(Transport subscriber, AeronPublisher<?> publisher) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (subscriber.isConnected() && publisher.isConnected()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Aeron publisher/subscriber did not connect within timeout");
    }
}
