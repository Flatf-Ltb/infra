package io.mercury.persistence.chronicle.queue;

import io.mercury.common.annotation.AbstractFunction;
import lombok.Getter;
import org.slf4j.Logger;

/**
 * 通用访问器抽象类
 *
 * @author yellow013
 */
public abstract class CloseableChronicleAccessor implements net.openhft.chronicle.core.io.Closeable {

    protected volatile boolean isClose = false;

    @Getter
    private final long allocateSeq;

    protected final String name;
    protected final Logger logger;

    protected CloseableChronicleAccessor(long allocateSeq, String name, Logger logger) {
        this.allocateSeq = allocateSeq;
        this.name = name;
        this.logger = logger;
    }

    @Override
    public void close() {
        this.isClose = true;
        close0();
    }

    @Override
    public boolean isClosed() {
        return isClose;
    }

    @AbstractFunction
    protected abstract void close0();

}
