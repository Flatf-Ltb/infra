package io.flatf.infra.transport.zmq;

import io.flatf.common.thread.Sleep;
import io.flatf.common.thread.Threads;
import io.flatf.infra.transport.api.Subscriber;
import io.flatf.infra.transport.attr.Topics;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class ZmqIpcPubSubTest {

    @Test
    public void test() {
        System.out.println("ZmqIpcPubSubTest Test Start");

        String topic = "ipc-test";

        Threads.startNewThread(() -> {
            try (Subscriber subscriber = ZmqConfig.ipc("test/01").ioThreads(1).createSubscriber(Topics.with(topic),
                    this::handleMag)) {
                subscriber.subscribe();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Sleep.millis(3000);

        try (ZmqPublisher<String> publisher = ZmqConfig.ipc("test/01").ioThreads(1)
                .createPublisherWithString(topic)) {
            Sleep.millis(2000);
            Random random = new Random();
            for (int i = 0; i < 20; i++) {
                publisher.publish(String.valueOf(random.nextInt()));
                Sleep.millis(200);
            }
            publisher.publish("end");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Sleep.millis(5000);

    }

    private void handleMag(byte[] topic, byte[] msg) {
        String str = new String(msg);
        System.out.println(new String(topic) + " -> " + str);
        if (str.equalsIgnoreCase("end"))
            System.exit(0);
    }

}
