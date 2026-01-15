package io.mercury.transport.zmq;

import io.mercury.common.thread.Sleep;
import io.mercury.common.thread.Threads;
import io.mercury.transport.api.Receiver;
import io.mercury.transport.zmq.exception.ZmqBindException;
import org.slf4j.Logger;
import org.zeromq.SocketType;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

import static io.mercury.common.lang.Validator.nonNull;
import static io.mercury.common.log4j2.Log4j2LoggerFactory.getLogger;

public class ZmqReceiver extends ZmqComponent implements Receiver, Closeable {

    private static final Logger log = getLogger(ZmqPublisher.class);

    private final Function<byte[], byte[]> handler;

    ZmqReceiver(@Nonnull ZmqCfg configurator,
                @Nonnull Function<byte[], byte[]> handler) {
        super(configurator);
        nonNull(handler, "handler");
        this.handler = handler;
        var addr = configurator.getAddr().fullUri();
        if (socket.bind(addr))
            log.info("ZmqReceiver bound addr -> {}", addr);
        else {
            log.error("ZmqReceiver unable to bind -> {}", addr);
            throw new ZmqBindException(addr);
        }
        setTcpKeepAlive(configurator.getTcpKeepAlive());
        this.name = "ZRecv$" + addr;
        newStartTime();
    }

    public Function<byte[], byte[]> getHandler() {
        return handler;
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.REP;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_RECEIVER;
    }

    private final byte[] emptyMsg = new byte[]{};

    @Override
    public void receive() {
        while (isRunning.get()) {
            byte[] recv = socket.recv();
            byte[] reply = handler.apply(recv);
            if (reply != null)
                socket.send(reply);
            else
                socket.send(emptyMsg);
        }
    }

    @Override
    public void reconnect() {
        throw new UnsupportedOperationException("ZmqReceiver unsupported reconnect function");
    }

    public static void main(String[] args) {
        try (ZmqReceiver receiver = ZmqCfg.tcp(5551).createReceiver((byte[] recvMsg) -> {
            System.out.println(new String(recvMsg));
            return null;
        })) {
            Sleep.millis(15000);
            Threads.startNewThread(receiver::receive);
        } catch (IOException ignored) {
        }
    }

}
