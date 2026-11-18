package io.flatf.infra.transport.zmq;

import io.flatf.infra.transport.attr.Topics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ZmqPublisher} / {@link ZmqSubscriber} 的契约测试 (PLAN_命令通道PUB-SUB与适配器租约 §5 第 1 步, §8)。
 *
 * <p>写成 JUnit 5：本模块没有 {@code junit-vintage-engine}，同包里的 JUnit 4 PUB/SUB 用例从不执行。
 * PUB/SUB 有慢加入问题，订阅生效前发出的消息会被丢弃，所以每个用例先发探测消息直到订阅方收到，再开始断言。
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class ZmqPubSubTest {

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
     * 反复发送探测消息直到每个订阅方都收到, 用来跨过慢加入; 然后清空所有队列。探测消息的主题为 {@code topic}。
     */
    @SafeVarargs
    private static void warmUp(ZmqPublisher<String> pub, String topic, BlockingQueue<String[]>... queues)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        boolean[] ready = new boolean[queues.length];
        int remaining = queues.length;
        while (remaining > 0 && System.nanoTime() < deadline) {
            pub.publish(utf8(topic), "warmup");
            Thread.sleep(50);
            for (int i = 0; i < queues.length; i++) {
                if (!ready[i] && !queues[i].isEmpty()) {
                    ready[i] = true;
                    remaining--;
                }
            }
        }
        if (remaining > 0)
            throw new AssertionError("subscription never became effective");
        Thread.sleep(200);
        for (BlockingQueue<String[]> q : queues)
            q.clear();
    }

    private static Thread startSubscriber(ZmqSubscriber subscriber, AtomicReference<Throwable> thrown) {
        // 守护线程: 接收循环卡住时不阻止 JVM 退出, 失败表现为用例超时而不是整个构建挂起
        return Thread.ofPlatform().name("test-zmq-sub").daemon(true).start(() -> {
            try {
                subscriber.subscribe();
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
    }

    @Test
    public void shouldApplyConfiguredHighWaterMark() throws Exception {
        int port = freePort();
        try (ZmqPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port).highWaterMark(4096)
                .createPublisherWithString();
             ZmqSubscriber sub = ZmqConfig.tcp("127.0.0.1", port).highWaterMark(2048)
                     .createSubscriber(Topics.with(""), (t, m) -> {
                     })) {
            assertEquals(4096, pub.getSocket().getSndHWM());
            assertEquals(2048, sub.getSocket().getRcvHWM());
        }
    }

    /**
     * 前缀定向：订阅 {@code A|} 的一方只收到 {@code A|}，收不到 {@code AB|}；订阅空前缀的旁听方收到全部。
     */
    @Test
    public void shouldFilterByTopicPrefixWithSeparator() throws Exception {
        int port = freePort();
        BlockingQueue<String[]> adapterA = new LinkedBlockingQueue<>();
        BlockingQueue<String[]> observer = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port).createPublisherWithString();
             ZmqSubscriber subA = ZmqConfig.tcp("127.0.0.1", port).createSubscriber(Topics.with("A|"),
                     (t, m) -> adapterA.add(new String[]{str(t), str(m)}));
             ZmqSubscriber subAll = ZmqConfig.tcp("127.0.0.1", port).createSubscriber(Topics.with(""),
                     (t, m) -> observer.add(new String[]{str(t), str(m)}))) {
            startSubscriber(subA, thrown);
            startSubscriber(subAll, thrown);
            warmUp(pub, "A|", adapterA, observer);

            pub.publish(utf8("AB|"), "{ab}");
            pub.publish(utf8("A|"), "{a}");

            String[] a = adapterA.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
            assertNotNull(a);
            assertEquals("A|", a[0]);
            assertEquals("{a}", a[1]);
            assertNull(adapterA.poll(300, TimeUnit.MILLISECONDS), "'A|' must not receive 'AB|'");

            String[] first = observer.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
            String[] second = observer.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
            assertNotNull(first);
            assertNotNull(second);
            assertEquals("AB|", first[0]);
            assertEquals("A|", second[0]);
        }
        assertNull(thrown.get());
    }

    /**
     * 单帧消息 (主题在帧内) 交给消费者时主题为空数组、消息为整帧；不能与下一条消息拼在一起。
     */
    @Test
    public void shouldDeliverSingleFrameMessagesWithoutPairingThem() throws Exception {
        int port = freePort();
        BlockingQueue<String[]> received = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZContext ctx = new ZContext();
             ZmqSubscriber sub = ZmqConfig.tcp("127.0.0.1", port).createSubscriber(Topics.with("A|"),
                     (t, m) -> received.add(new String[]{str(t), str(m)}))) {
            ZMQ.Socket rawPub = ctx.createSocket(SocketType.PUB);
            rawPub.bind("tcp://127.0.0.1:" + port);
            startSubscriber(sub, thrown);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
            String[] got = null;
            while (got == null && System.nanoTime() < deadline) {
                rawPub.send("A|warmup");
                got = received.poll(50, TimeUnit.MILLISECONDS);
            }
            assertNotNull(got, "subscription never became effective");
            Thread.sleep(100);
            received.clear();

            rawPub.send("A|{one}");
            rawPub.send("A|{two}");

            String[] one = received.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
            String[] two = received.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
            assertNotNull(one);
            assertNotNull(two);
            assertEquals("", one[0]);
            assertEquals("A|{one}", one[1]);
            assertEquals("", two[0]);
            assertEquals("A|{two}", two[1]);
        }
        assertNull(thrown.get());
    }

    /**
     * 多线程同时发送两帧消息时，主题帧与主体帧不能串到别的消息上。
     */
    @Test
    public void shouldNotInterleaveFramesUnderConcurrentPublish() throws Exception {
        int port = freePort();
        int threads = 4;
        int perThread = 2000;
        BlockingQueue<String[]> received = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqPublisher<String> pub = ZmqConfig.tcp("127.0.0.1", port).highWaterMark(100_000)
                .createPublisherWithString();
             ZmqSubscriber sub = ZmqConfig.tcp("127.0.0.1", port).highWaterMark(100_000)
                     .createSubscriber(Topics.with(""), (t, m) -> received.add(new String[]{str(t), str(m)}))) {
            startSubscriber(sub, thrown);
            warmUp(pub, "T", received);

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

    /**
     * subscribe() 阻塞时从另一线程 close：接收线程正常返回，不抛异常。
     */
    @Test
    public void shouldExitSubscribeLoopCleanlyOnClose() throws Exception {
        int port = freePort();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        try (ZmqPublisher<String> ignored = ZmqConfig.tcp("127.0.0.1", port).createPublisherWithString()) {
            ZmqSubscriber sub = ZmqConfig.tcp("127.0.0.1", port).createSubscriber(Topics.with(""), (t, m) -> {
            });
            Thread loop = startSubscriber(sub, thrown);
            Thread.sleep(200);
            sub.close();
            loop.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
            assertFalse(loop.isAlive(), "subscribe loop must return after close");
        }
        assertNull(thrown.get(), () -> "subscribe loop threw " + thrown.get());
    }
}
