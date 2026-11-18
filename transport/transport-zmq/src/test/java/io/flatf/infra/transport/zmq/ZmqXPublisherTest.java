package io.flatf.infra.transport.zmq;

import io.flatf.infra.transport.attr.Topics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ZmqXPublisher} 的契约测试 (PLAN_命令通道PUB-SUB与适配器租约 §8.5, Q4 闸门)。
 *
 * <p>闸门依赖的是这几条: 订阅生效会以事件报告出来; 报告之后立刻发出的消息不会因慢加入被丢;
 * 同一主题只在第一个订阅者到来和最后一个离开时报告; 订阅者先于发布者启动也一样。
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class ZmqXPublisherTest {

    private static final long AWAIT_SECONDS = 10L;

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 订阅事件记为 "+topic", 退订记为 "-topic"。
     */
    private static ZmqXPublisher.SubscriptionListener recordTo(BlockingQueue<String> events) {
        return (subscribed, topic) -> events.add((subscribed ? "+" : "-") + str(topic));
    }

    private static ZmqSubscriber subscriber(int port, String topic, BlockingQueue<String[]> received) {
        return ZmqConfig.tcp("127.0.0.1", port).createSubscriber(Topics.with(topic),
                (t, m) -> received.add(new String[]{str(t), str(m)}));
    }

    private static void startSubscriber(ZmqSubscriber subscriber, AtomicReference<Throwable> thrown) {
        Thread.ofPlatform().name("test-zmq-sub").daemon(true).start(() -> {
            try {
                subscriber.subscribe();
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
    }

    @Test
    public void shouldReportSubscribeAndUnsubscribe() throws Exception {
        int port = freePort();
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqXPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port)
                .createXPublisher("", String::getBytes, recordTo(events))) {
            pub.start();
            ZmqSubscriber sub = subscriber(port, "A|", new LinkedBlockingQueue<>());
            startSubscriber(sub, thrown);
            assertEquals("+A|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS));
            sub.close();
            assertEquals("-A|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS));
        }
        assertNull(thrown.get());
    }

    /**
     * 收到订阅事件后立即发送, 不做任何预热, 消息必须送达。重复多次, 每次换一个新订阅者。
     */
    @Test
    public void shouldDeliverMessageSentRightAfterSubscriptionReported() throws Exception {
        int port = freePort();
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqXPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port)
                .createXPublisher("", String::getBytes, recordTo(events))) {
            pub.start();
            for (int round = 0; round < 20; round++) {
                BlockingQueue<String[]> received = new LinkedBlockingQueue<>();
                try (ZmqSubscriber sub = subscriber(port, "A|", received)) {
                    startSubscriber(sub, thrown);
                    assertEquals("+A|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS), "round " + round);
                    pub.publish(utf8("A|"), "{cmd-" + round + "}");
                    String[] msg = received.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
                    assertNotNull(msg, "round " + round + ": message sent after subscription was lost");
                    assertEquals("{cmd-" + round + "}", msg[1]);
                }
                assertEquals("-A|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS), "round " + round);
            }
        }
        assertNull(thrown.get());
    }

    /**
     * 订阅者先启动 (PUB 尚未 bind, SUB 在后台重连), 发布者后启动: 仍以订阅事件为准, 事件之后的消息送达。
     */
    @Test
    public void shouldReportSubscriptionWhenSubscriberStartsFirst() throws Exception {
        int port = freePort();
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        BlockingQueue<String[]> received = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqSubscriber sub = subscriber(port, "A|", received)) {
            startSubscriber(sub, thrown);
            Thread.sleep(300);
            try (ZmqXPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port)
                    .createXPublisher("", String::getBytes, recordTo(events))) {
                pub.start();
                assertEquals("+A|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS));
                pub.publish(utf8("A|"), "{first}");
                String[] msg = received.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
                assertNotNull(msg);
                assertEquals("{first}", msg[1]);
            }
        }
        assertNull(thrown.get());
    }

    /**
     * 同一主题两个订阅者 (如 adapter 重启时新旧实例重叠): 只在第一个到来时报告订阅, 最后一个离开时报告退订。
     */
    @Test
    public void shouldReportOnlyFirstSubscribeAndLastUnsubscribeOfSameTopic() throws Exception {
        int port = freePort();
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqXPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port)
                .createXPublisher("", String::getBytes, recordTo(events))) {
            pub.start();
            ZmqSubscriber first = subscriber(port, "A|", new LinkedBlockingQueue<>());
            startSubscriber(first, thrown);
            assertEquals("+A|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS));

            BlockingQueue<String[]> secondReceived = new LinkedBlockingQueue<>();
            ZmqSubscriber second = subscriber(port, "A|", secondReceived);
            startSubscriber(second, thrown);
            // 第二个订阅者没有事件; 用消息送达确认它的订阅已经生效
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
            while (secondReceived.isEmpty() && System.nanoTime() < deadline) {
                pub.publish(utf8("A|"), "probe");
                Thread.sleep(50);
            }
            assertFalse(secondReceived.isEmpty(), "second subscription never became effective");
            assertNull(events.poll(300, TimeUnit.MILLISECONDS), "duplicate subscription must not be reported");

            first.close();
            assertNull(events.poll(500, TimeUnit.MILLISECONDS), "unsubscribe must wait for the last subscriber");
            second.close();
            assertEquals("-A|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS));
        }
        assertNull(thrown.get());
    }

    /**
     * 回调抛异常不能杀死轮询线程, 之后的订阅事件照常报告。
     */
    @Test
    public void shouldKeepPollingWhenListenerThrows() throws Exception {
        int port = freePort();
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        AtomicBoolean thrownOnce = new AtomicBoolean(false);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        ZmqXPublisher.SubscriptionListener listener = (subscribed, topic) -> {
            if (thrownOnce.compareAndSet(false, true))
                throw new IllegalStateException("listener failure for test");
            events.add((subscribed ? "+" : "-") + str(topic));
        };
        try (ZmqXPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port)
                .createXPublisher("", String::getBytes, listener);
             ZmqSubscriber a = subscriber(port, "A|", new LinkedBlockingQueue<>())) {
            pub.start();
            startSubscriber(a, thrown);
            Thread.sleep(300);
            assertTrue(thrownOnce.get());
            try (ZmqSubscriber b = subscriber(port, "B|", new LinkedBlockingQueue<>())) {
                startSubscriber(b, thrown);
                assertEquals("+B|", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS));
            }
        }
        assertNull(thrown.get());
    }

    /**
     * 轮询线程与多个发送线程共用套接字时, 两帧消息不能串帧。
     */
    @Test
    public void shouldNotInterleaveFramesUnderConcurrentPublish() throws Exception {
        int port = freePort();
        int threads = 4;
        int perThread = 2000;
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        BlockingQueue<String[]> received = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqXPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port).highWaterMark(100_000)
                .createXPublisher("", String::getBytes, recordTo(events));
             ZmqSubscriber sub = ZmqConfig.tcp("127.0.0.1", port).highWaterMark(100_000)
                     .createSubscriber(Topics.with("T"), (t, m) -> received.add(new String[]{str(t), str(m)}))) {
            pub.start();
            startSubscriber(sub, thrown);
            assertEquals("+T", events.poll(AWAIT_SECONDS, TimeUnit.SECONDS));

            CountDownLatch start = new CountDownLatch(1);
            List<Thread> senders = new CopyOnWriteArrayList<>();
            for (int i = 0; i < threads; i++) {
                String topic = "T" + i + "|";
                senders.add(Thread.ofPlatform().daemon(true).start(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int n = 0; n < perThread; n++)
                        pub.publish(utf8(topic), topic + n);
                }));
            }
            start.countDown();
            for (Thread t : senders)
                t.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));

            int count = 0;
            String[] msg;
            while (count < threads * perThread && (msg = received.poll(AWAIT_SECONDS, TimeUnit.SECONDS)) != null) {
                assertTrue(msg[1].startsWith(msg[0]), "frames interleaved: topic=" + msg[0] + " body=" + msg[1]);
                count++;
            }
            assertEquals(threads * perThread, count);
        }
        assertNull(thrown.get());
    }

    @Test
    public void shouldStopPollThreadOnClose() throws Exception {
        int port = freePort();
        ZmqXPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port)
                .createXPublisher("", String::getBytes, (subscribed, topic) -> {
                });
        pub.start();
        Thread.sleep(100);
        String pollerName = pub.getName() + "-subscription-poller";
        assertTrue(Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals(pollerName)));
        pub.close();
        assertFalse(Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.getName().equals(pollerName) && t.isAlive()));
    }
}
