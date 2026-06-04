package io.flatf.infra.transport.aeron;

import io.aeron.Aeron;
import io.flatf.infra.TransportComponent;
import io.flatf.infra.api.Transport;
import org.agrona.CloseHelper;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Base class for Aeron transport components.
 */
abstract class AeronComponent extends TransportComponent implements Transport, Closeable {

    private static final Logger log = getLogger(AeronComponent.class);

    protected final AeronConfig cfg;
    protected final Aeron aeron;
    protected final AtomicBoolean isRunning = new AtomicBoolean(true);
    protected String name;

    AeronComponent(@Nonnull AeronConfig cfg) {
        nonNull(cfg, "cfg");
        this.cfg = cfg;
        this.aeron = cfg.newAeron();
    }

    public abstract AeronType getAeronType();

    protected abstract void closeResources();

    protected abstract boolean isTransportConnected();

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isConnected() {
        return isRunning.get() && !aeron.isClosed() && isTransportConnected();
    }

    @Override
    public boolean closeIgnoreException() {
        if (!isRunning.compareAndSet(true, false)) {
            log.warn("Aeron component -> {} already closed", name);
            return false;
        }

        try {
            closeResources();
        } catch (Exception e) {
            log.warn("Aeron component -> {} closeResources failed: {}", name, e.getMessage(), e);
        } finally {
            CloseHelper.quietClose(aeron);
            cfg.onAeronClosed();
            newEndTime();
            log.info("Aeron component -> {} closed, duration={}ms", name, getRunningDuration());
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        closeIgnoreException();
    }
}
