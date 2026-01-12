package io.mercury.transport.zmq;

import io.mercury.common.serialization.specific.BytesSerializer;
import io.mercury.transport.api.Sender;
import io.mercury.transport.zmq.exception.ZmqConnectionException;
import org.slf4j.Logger;
import org.zeromq.SocketType;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;

import static io.mercury.common.lang.Validator.nonNull;
import static io.mercury.common.log4j2.Log4j2LoggerFactory.getLogger;

@NotThreadSafe
public class ZmqSender<T> extends ZmqComponent implements Sender<T>, Closeable {

    private static final Logger log = getLogger(ZmqSender.class);

    private final BytesSerializer<T> serializer;

    /**
     * @param configurator ZmqConfigurator
     * @param serializer   BytesSerializer<T>
     */
    ZmqSender(@Nonnull ZmqConfigurator configurator,
              @Nonnull BytesSerializer<T> serializer) {
        super(configurator);
        nonNull(serializer, "serializer");
        this.serializer = serializer;
        var addr = configurator.getAddr().fullUri();
        if (socket.connect(addr)) {
            log.info("ZmqSender connected addr -> {}", addr);
        } else {
            log.error("ZmqSender unable to connect addr -> {}", addr);
            throw new ZmqConnectionException(addr);
        }
        this.name = "ZSender$" + addr;
        newStartTime();
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.REQ;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_SENDER;
    }

    @Override
    public void send(T msg) {
        byte[] bytes = serializer.serialize(msg);
        if (bytes != null && bytes.length > 0) {
            socket.send(bytes);
            socket.recv();
        }
    }

    public static void main(String[] args) {

        ZmqConfigurator cfg = ZmqConfigurator.tcp("localhost", 5551);
        try (ZmqSender<String> sender = new ZmqSender<>(cfg, String::getBytes)) {
            sender.send("TEST MSG");
        } catch (IOException ignored) {
        }

    }

}
