package io.flatf.infra.persistence.chronicle.queue.multitype;

import io.flatf.common.annotation.AbstractFunction;
import io.flatf.common.codec.Envelope;
import io.flatf.common.serialization.api.Serializer;
import io.flatf.infra.persistence.chronicle.exception.ChronicleAppendException;
import io.flatf.infra.persistence.chronicle.queue.AbstractChronicleAppender;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Supplier;

import static io.flatf.common.datetime.DateTimeUtil.fmtDateTime;
import static io.flatf.common.datetime.pattern.impl.DateTimePattern.YY_MM_DD_HH_MM_SS_SSS;

@Immutable
@NotThreadSafe
public abstract class AbstractChronicleMultitypeAppender<E extends Envelope, IN>
        extends AbstractChronicleAppender<IN> implements Runnable {

    protected AbstractChronicleMultitypeAppender(long allocateSeq,
                                                 String name,
                                                 Logger logger,
                                                 ExcerptAppender appender,
                                                 Supplier<IN> dataProducer) {
        super(allocateSeq, name, logger, appender, dataProducer);
    }

    /**
     * @param t IN
     * @throws IllegalStateException    ise
     * @throws ChronicleAppendException cae
     */
    public void append(@Nonnull E envelope, @Nullable IN t)
            throws IllegalStateException, ChronicleAppendException {
        if (isClose) {
            throw new IllegalStateException("Unable to append data, Chronicle queue is closed");
        }
        try {
            if (t != null) {
                append0(envelope, t);
            } else {
                logger.warn("Appender -> {} : received null object, Not written to the queue.", getAppenderName());
            }
        } catch (Exception e) {
            throw new ChronicleAppendException(e.getMessage(), e);
        }
    }

    /**
     * @param obj        Object
     * @param serializer Serializer<Object, IN>
     * @throws IllegalStateException    ise
     * @throws ChronicleAppendException cae
     */
    public void append(@Nonnull E envelope,
                       @Nonnull Object obj,
                       Serializer<Object, IN> serializer)
            throws IllegalStateException, ChronicleAppendException {
        append(envelope, serializer.serialize(obj));
    }

    @AbstractFunction
    protected abstract void append0(@Nullable E envelope, @Nonnull IN t);

    @Override
    protected void append0(@Nonnull IN in) {
        append0(envelope, in);
    }

    @Nullable
    private E envelope;

    /**
     * 设置默认信封
     *
     * @param envelope E
     */
    public void setEnvelope(@Nonnull E envelope) {
        this.envelope = envelope;
    }

    @Override
    public void run() {
        if (dataProducer == null) {
            logger.error("Supplier is null, Thread exit");
            return;
        }
        if (envelope != null) {
            for (; ; ) {
                if (isClose) {
                    logger.info("Chronicle queue is closed, {} Thread exit", getAppenderName());
                    break;
                } else {
                    IN t = dataProducer.get();
                    if (t != null)
                        append(envelope, t);
                }
            }
        } else {
            logger.error("MultitypeAppender -> [{}] Default envelope is null, Thread exit at {}",
                    getAppenderName(), fmtDateTime(YY_MM_DD_HH_MM_SS_SSS));
        }
    }

}
