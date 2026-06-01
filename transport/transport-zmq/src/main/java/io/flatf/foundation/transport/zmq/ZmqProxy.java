package io.flatf.foundation.transport.zmq;

import io.flatf.foundation.common.log4j2.Log4j2LoggerFactory;
import io.flatf.foundation.transport.TransportComponent;
import io.flatf.foundation.transport.zmq.exception.ZmqProxyException;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static io.flatf.foundation.common.lang.Validator.nonNull;

public final class ZmqProxy extends TransportComponent {

    private static final Logger log = Log4j2LoggerFactory.getLogger(ZmqProxy.class);

    private final String name;

    private final boolean isConnected;

    private final ZmqComponent frontend;

    private final ZmqComponent backend;

    private ZmqComponent capture;

    private ZmqSender<String> control;

    ZmqProxy(@Nonnull ZmqComponent frontend, @Nonnull ZmqComponent backend,
             @Nullable ZmqComponent capture, @Nullable ZmqComponent control) {
        nonNull(frontend, "frontend");
        nonNull(backend, "backend");
        this.frontend = frontend;
        this.backend = backend;
        this.name = "[" + frontend.getName() + "]->[" + backend.getName() + "]";
        try {
            this.isConnected = ZMQ.proxy(
                    // 代理前端
                    frontend.getSocket(),
                    // 代理后端
                    backend.getSocket(),
                    // 捕获通道
                    capture != null ? capture.getSocket() : null,
                    // 控制通道
                    control != null ? control.getSocket() : null);
        } catch (Exception e) {
            throw new ZmqProxyException("ZMQ.proxy(frontend, backend, capture, control) -> " + e.getMessage(), e);
        }
        log.info("ZmqProxy -> {}", name);
        newStartTime();
    }

    public ZmqComponent getFrontend() {
        return frontend;
    }

    public ZmqComponent getBackend() {
        return backend;
    }

    public ZmqComponent getCapture() {
        return capture;
    }

    public ZmqSender<String> getControl() {
        return control;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    @Override
    public boolean closeIgnoreException() {
        return false;
    }

    /**
     * @param frontend 代理前端
     * @param backend  代理后端
     * @return Builder
     */
    public static Builder config(ZmqComponent frontend, ZmqComponent backend) {
        return new Builder(frontend, backend);
    }

    public static class Builder {
        // 代理前端
        private final ZmqComponent frontend;
        // 代理后端
        private final ZmqComponent backend;

        // 捕获通道
        private ZmqComponent capture;
        // 控制通道
        private ZmqComponent control;

        private Builder(ZmqComponent frontend, ZmqComponent backend) {
            this.frontend = frontend;
            this.backend = backend;
        }

        /**
         * If the capture socket is not NULL, the proxy shall send all messages,<br>
         * received on both frontend and backend, to the capture socket. <br>
         * The capture socket should be a ZMQ_PUB, ZMQ_DEALER, ZMQ_PUSH, or ZMQ_PAIR
         * socket.
         * <p>
         * 代理将发送所有前端和后端接收到的消息到捕获Socket. 捕获Socket应该为ZMQ_PUB, ZMQ_DEALER,
         * ZMQ_PUSH或ZMQ_PAIR类型的套接字
         *
         * @param capture 捕获Socket, 用于接收全部数据
         * @return Builder
         */
        public Builder setCapture(ZmqComponent capture) {
            this.capture = capture;
            return this;
        }

        /**
         * @param control 控制通道, 用于接收控制指令
         * @return Builder
         */
        public Builder setControl(ZmqComponent control) {
            this.control = control;
            return this;
        }

        public ZmqProxy build() {
            return new ZmqProxy(frontend, backend, capture, control);
        }

    }

}
