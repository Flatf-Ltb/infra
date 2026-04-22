package io.flatf.transport.aeron;

import io.aeron.Aeron;
import io.flatf.transport.TransportComponent;
import io.flatf.transport.api.Transport;
import org.agrona.CloseHelper;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron 组件抽象基类, 对应 ZMQ 中的 {@code ZmqComponent}.
 *
 * <p>每个组件持有独立的 {@link Aeron} 客户端连接, 与 ZMQ 每个组件持有独立 ZContext 的方式一致.
 * {@link Aeron} 实例通过 {@link AeronCfg#newAeron()} 创建, MediaDriver 由 {@link AeronCfg} 负责管理
 * (JVM 内共享单例).
 */
abstract class AeronComponent extends TransportComponent implements Transport, Closeable {

    private static final Logger log = getLogger(AeronComponent.class);

    protected final AeronCfg cfg;
    protected final Aeron aeron;
    protected final AtomicBoolean isRunning = new AtomicBoolean(true);
    protected String name;

    AeronComponent(@Nonnull AeronCfg cfg) {
        nonNull(cfg, "cfg");
        this.cfg = cfg;
        this.aeron = cfg.newAeron();
    }

    public AeronType getAeronType() {
        return null;
    }

    /**
     * 子类实现: 关闭 Publication 或 Subscription 等 Aeron 资源.
     * 由 {@link #closeIgnoreException()} 在停止 Aeron 客户端之前调用.
     */
    protected abstract void closeResources();

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isConnected() {
        return !aeron.isClosed();
    }

    @Override
    public boolean closeIgnoreException() {
        if (isRunning.compareAndSet(true, false)) {
            closeResources();
            CloseHelper.quietClose(aeron);
            newEndTime();
            log.info("Aeron component -> {} closed, duration={}ms", name, getRunningDuration());
            return true;
        } else {
            log.warn("Aeron component -> {} already closed", name);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        closeIgnoreException();
    }

}