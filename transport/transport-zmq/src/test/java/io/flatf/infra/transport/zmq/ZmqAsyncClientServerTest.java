package io.flatf.infra.transport.zmq;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ZmqAsyncClient} / {@link ZmqAsyncServer} 的往返测试。
 *
 * <p>用例绑定临时端口并带超时，不使用 {@code System.exit} 或固定长睡眠 —— 本包里更早的
 * PUB/SUB 演示型用例是那种写法，不要照抄。
 *
 * <p>写成 JUnit 5：本模块只引入了 {@code junit-jupiter}，没有 {@code junit-vintage-engine}，
 * 因此现存的 JUnit 4 用例实际上一条都不会被执行。
 */
public class ZmqAsyncClientServerTest {

    private static final long AWAIT_SECONDS = 10L;

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static ZmqConfig serverConfig(int port) {
        return ZmqConfig.tcp("*", port);
    }

    private static ZmqConfig clientConfig(int port) {
        return ZmqConfig.tcp("127.0.0.1", port);
    }

    @Test
    public void shouldRoundTripRequestAndReply() throws Exception {
        int port = freePort();
        CountDownLatch replied = new CountDownLatch(1);
        AtomicReference<String> replyRef = new AtomicReference<>();
        AtomicReference<String> senderIdRef = new AtomicReference<>();

        try (ZmqAsyncServer server = serverConfig(port).createAsyncServer(exchange -> {
            senderIdRef.set(exchange.senderId());
            exchange.reply("ACK:" + new String(exchange.payload(), StandardCharsets.UTF_8));
        })) {
            server.start();

            try (ZmqAsyncClient client = clientConfig(port).createAsyncClient("engine-1", reply -> {
                replyRef.set(new String(reply, StandardCharsets.UTF_8));
                replied.countDown();
            })) {
                client.start();

                assertEquals(ZmqAsyncClient.SendResult.SENT, client.send("NEW_ORDER:req-1"));
                assertTrue(replied.await(AWAIT_SECONDS, TimeUnit.SECONDS), "reply did not arrive");
                assertEquals("ACK:NEW_ORDER:req-1", replyRef.get());
                assertEquals("engine-1", senderIdRef.get(), "explicit identity must reach the server");
            }
        }
    }

    /**
     * DEALER 不做 send-recv 轮转：连续发多条不必等待应答，这正是它取代 REQ/REP 的理由。
     */
    @Test
    public void shouldPipelineMultipleRequestsWithoutWaitingForReplies() throws Exception {
        int port = freePort();
        int count = 50;
        CountDownLatch replied = new CountDownLatch(count);
        ConcurrentLinkedQueue<String> replies = new ConcurrentLinkedQueue<>();

        try (ZmqAsyncServer server = serverConfig(port).createAsyncServer(exchange ->
                exchange.reply(new String(exchange.payload(), StandardCharsets.UTF_8)))) {
            server.start();

            try (ZmqAsyncClient client = clientConfig(port).createAsyncClient(reply -> {
                replies.add(new String(reply, StandardCharsets.UTF_8));
                replied.countDown();
            })) {
                client.start();

                for (int i = 0; i < count; i++)
                    assertEquals(ZmqAsyncClient.SendResult.SENT, client.send("req-" + i));

                assertTrue(replied.await(AWAIT_SECONDS, TimeUnit.SECONDS), "not all replies arrived");
                assertEquals(count, replies.size());
                // DEALER/ROUTER 保序：单一对端下应答顺序与请求顺序一致
                List<String> ordered = List.copyOf(replies);
                assertEquals("req-0", ordered.getFirst());
                assertEquals("req-" + (count - 1), ordered.getLast());
            }
        }
    }

    /**
     * 服务端可以延后应答，而不是被迫在处理回调里同步答复。这条约束存在的理由是：
     * 强制同步应答会让一次慢处理顶住整条接收通道。
     */
    @Test
    public void shouldAllowReplyFromAnotherThreadAfterHandlerReturns() throws Exception {
        int port = freePort();
        CountDownLatch replied = new CountDownLatch(1);
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<String> replyRef = new AtomicReference<>();
        AtomicReference<ZmqAsyncServer.Exchange> deferred = new AtomicReference<>();

        try (ZmqAsyncServer server = serverConfig(port).createAsyncServer(exchange -> {
            deferred.set(exchange);
            received.countDown();
        })) {
            server.start();

            try (ZmqAsyncClient client = clientConfig(port).createAsyncClient(reply -> {
                replyRef.set(new String(reply, StandardCharsets.UTF_8));
                replied.countDown();
            })) {
                client.start();
                assertEquals(ZmqAsyncClient.SendResult.SENT, client.send("deferred-req"));
                assertTrue(received.await(AWAIT_SECONDS, TimeUnit.SECONDS), "server never received the request");

                // 处理回调早已返回，从测试线程补发应答
                assertTrue(deferred.get().reply("DEFERRED_ACK"));
                assertTrue(replied.await(AWAIT_SECONDS, TimeUnit.SECONDS), "deferred reply did not arrive");
                assertEquals("DEFERRED_ACK", replyRef.get());
            }
        }
    }

    /**
     * ROUTER 为每个对端保留身份，因此一条通道能同时服务多个请求端 —— 而 PUB/SUB 做不到定向应答。
     */
    @Test
    public void shouldRouteRepliesBackToTheOriginatingClient() throws Exception {
        int port = freePort();
        CountDownLatch bothReplied = new CountDownLatch(2);
        AtomicReference<String> firstReply = new AtomicReference<>();
        AtomicReference<String> secondReply = new AtomicReference<>();

        try (ZmqAsyncServer server = serverConfig(port).createAsyncServer(exchange ->
                exchange.reply(exchange.senderId() + ":"
                        + new String(exchange.payload(), StandardCharsets.UTF_8)))) {
            server.start();

            try (ZmqAsyncClient first = clientConfig(port).createAsyncClient("client-a", reply -> {
                firstReply.set(new String(reply, StandardCharsets.UTF_8));
                bothReplied.countDown();
            });
                 ZmqAsyncClient second = clientConfig(port).createAsyncClient("client-b", reply -> {
                     secondReply.set(new String(reply, StandardCharsets.UTF_8));
                     bothReplied.countDown();
                 })) {
                first.start();
                second.start();

                assertEquals(ZmqAsyncClient.SendResult.SENT, first.send("ping"));
                assertEquals(ZmqAsyncClient.SendResult.SENT, second.send("ping"));
                assertTrue(bothReplied.await(AWAIT_SECONDS, TimeUnit.SECONDS), "replies did not arrive");

                assertEquals("client-a:ping", firstReply.get());
                assertEquals("client-b:ping", secondReply.get());
            }
        }
    }

    /**
     * 关闭后发送必须给出明确结论，而不是静默丢弃 —— 静默丢弃正是 PUB/SUB 的问题所在。
     */
    @Test
    public void shouldReportClosedInsteadOfSilentlyDroppingAfterClose() throws Exception {
        int port = freePort();

        try (ZmqAsyncServer server = serverConfig(port).createAsyncServer(exchange -> exchange.reply("ACK"))) {
            server.start();

            ZmqAsyncClient client = clientConfig(port).createAsyncClient(reply -> {
            });
            client.start();
            assertEquals(ZmqAsyncClient.SendResult.SENT, client.send("before-close"));

            client.close();

            assertEquals(ZmqAsyncClient.SendResult.CLOSED, client.send("after-close"));
            assertFalse(client.send("after-close").isSent());
        }
    }

    /**
     * 关闭必须是幂等的，且不能挂住：轮询线程先停下再关套接字。
     */
    @Test
    public void shouldCloseIdempotentlyWithoutHanging() throws Exception {
        int port = freePort();

        ZmqAsyncServer server = serverConfig(port).createAsyncServer(exchange -> exchange.reply("ACK"));
        server.start();
        ZmqAsyncClient client = clientConfig(port).createAsyncClient(reply -> {
        });
        client.start();

        assertTimeoutPreemptively(java.time.Duration.ofSeconds(30L), () -> {
            client.close();
            client.close();
            server.close();
            server.close();
        });

        assertFalse(client.isConnected());
        assertFalse(server.isConnected());
    }

    /**
     * 请求端先于服务端启动时不能丢消息：DEALER 会把消息排在本地队列里，连上之后再投递。
     * PUB 在同样的场景下是直接丢弃。
     */
    @Test
    public void shouldQueueRequestsSentBeforeTheServerIsUp() throws Exception {
        int port = freePort();
        CountDownLatch replied = new CountDownLatch(1);
        AtomicReference<String> replyRef = new AtomicReference<>();

        try (ZmqAsyncClient client = clientConfig(port).createAsyncClient(reply -> {
            replyRef.set(new String(reply, StandardCharsets.UTF_8));
            replied.countDown();
        })) {
            client.start();
            assertEquals(ZmqAsyncClient.SendResult.SENT, client.send("early-req"),
                    "send before the server exists must still be accepted");

            try (ZmqAsyncServer server = serverConfig(port).createAsyncServer(exchange ->
                    exchange.reply("ACK:" + new String(exchange.payload(), StandardCharsets.UTF_8)))) {
                server.start();

                assertTrue(replied.await(AWAIT_SECONDS, TimeUnit.SECONDS), "queued request was never delivered");
                assertEquals("ACK:early-req", replyRef.get());
            }
        }
    }
}
