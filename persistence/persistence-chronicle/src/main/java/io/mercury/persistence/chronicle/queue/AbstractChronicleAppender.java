package io.mercury.persistence.chronicle.queue;

import io.mercury.common.annotation.AbstractFunction;
import io.mercury.common.annotation.thread.OnlyAllowSingleThreadAccess;
import io.mercury.common.serialization.api.Serializer;
import io.mercury.persistence.chronicle.exception.ChronicleAppendException;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Supplier;

@Immutable
@NotThreadSafe
public abstract class AbstractChronicleAppender<T> extends CloseableChronicleAccessor
        implements Runnable {

    protected final ExcerptAppender appender;

    protected final Supplier<T> dataProducer;

    protected AbstractChronicleAppender(long allocateSeq, String name,
                                        Logger logger, ExcerptAppender appender,
                                        Supplier<T> dataProducer) {
        super(allocateSeq, name, logger);
        this.appender = appender;
        this.dataProducer = dataProducer;
    }

    public ExcerptAppender getExcerptAppender() {
        return appender;
    }

    public int getCycle() {
        return appender.cycle();
    }

    public int getSourceId() {
        return appender.sourceId();
    }

    public String getAppenderName() {
        return name;
    }

    /**
     * @param in          IN
     * @param serializer Serializer<IN, T>
     * @throws IllegalStateException    ise
     * @throws ChronicleAppendException cae
     */
    @OnlyAllowSingleThreadAccess
    public <IN> void append(@Nullable IN in, @Nonnull Serializer<IN, T> serializer)
            throws IllegalStateException, ChronicleAppendException {
        append(serializer.serialize(in));
    }

    /**
     * @param in IN
     * @throws IllegalStateException    ise
     * @throws ChronicleAppendException cae
     */
    @OnlyAllowSingleThreadAccess
    public void append(@Nullable T in) throws IllegalStateException, ChronicleAppendException {
        if (isClose) {
            throw new IllegalStateException("Unable to append data, Chronicle queue is closed");
        }
        try {
            if (in != null) {
                append0(in);
            } else {
                logger.warn("ChronicleAppender -> [{}] : received null object, Not written to the queue", getAppenderName());
            }
        } catch (Exception e) {
            throw new ChronicleAppendException(e.getMessage(), e);
        }
    }

    @AbstractFunction
    protected abstract void append0(@Nonnull T in);

    @Override
    public void run() {
        if (dataProducer != null) {
            for (; ; ) {
                if (!isClose) {
                    logger.info("Chronicle queue is closed, {} Thread exit", getAppenderName());
                    break;
                } else {
                    T in = dataProducer.get();
                    append(in);
                }
            }
        } else {
            logger.warn("Data producer function is null, Thread exit");
        }
    }

    protected void close0() {
        logger.info("Appender -> {} is closed.", getAppenderName());
    }

}
