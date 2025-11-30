package io.mercury.persistence.chronicle.queue;

import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;

import io.mercury.common.lang.Asserter;
import io.mercury.common.sequence.EpochSequence;
import io.mercury.persistence.chronicle.queue.ChronicleDocumentQueue.ChronicleDocumentAppender;
import io.mercury.persistence.chronicle.queue.ChronicleDocumentQueue.ChronicleDocumentReader;
import io.mercury.persistence.chronicle.queue.params.ReaderParams;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.Marshallable;

@Immutable
public class ChronicleDocumentQueue<T extends Marshallable>
        extends AbstractChronicleQueue<T, ChronicleDocumentAppender<T>, ChronicleDocumentReader<T>> {

    private final Supplier<T> marshallableSupplier;

    private ChronicleDocumentQueue(DocumentQueueBuilder<T> builder) {
        super(builder);
        Asserter.nonNull(builder.marshallableSupplier, "builder.marshallableSupplier");
        this.marshallableSupplier = builder.marshallableSupplier;
    }

    public static <T extends Marshallable> DocumentQueueBuilder<T> newBuilder(Class<T> saveType) {
        return new DocumentQueueBuilder<>();
    }

    @Override
    protected ChronicleDocumentReader<T> createReader(@Nonnull String readerName,
                                                      @Nonnull ReaderParams readerParam,
                                                      @Nonnull Logger logger,
                                                      @Nonnull Consumer<T> consumer)
            throws IllegalStateException {
        return new ChronicleDocumentReader<>(EpochSequence.allocate(), readerName, fileCycle(), readerParam, logger,
                internalQueue.createTailer(), consumer, marshallableSupplier);
    }

    @Override
    protected ChronicleDocumentAppender<T> acquireAppender(@Nonnull String appenderName,
                                                           @Nonnull Logger logger,
                                                           @CheckForNull Supplier<T> dataProducer)
            throws IllegalStateException {
        return new ChronicleDocumentAppender<>(EpochSequence.allocate(), appenderName, logger,
                internalQueue.createAppender(), dataProducer);
    }

    /**
     * @author yellow013
     */
    public static final class DocumentQueueBuilder<T extends Marshallable>
            extends BaseQueueBuilder<DocumentQueueBuilder<T>> {

        private Supplier<T> marshallableSupplier;

        private DocumentQueueBuilder() {
        }

        public ChronicleDocumentQueue<T> build() {
            return new ChronicleDocumentQueue<>(this);
        }

        @Override
        protected DocumentQueueBuilder<T> self() {
            return this;
        }

        public DocumentQueueBuilder<T> setMarshallableSupplier(@Nonnull Supplier<T> marshallableSupplier) {
            this.marshallableSupplier = marshallableSupplier;
            return this;
        }

    }

    @Immutable
    @NotThreadSafe
    public static final class ChronicleDocumentAppender<T extends Marshallable> extends AbstractChronicleAppender<T> {

        ChronicleDocumentAppender(long allocateSeq,
                                  String appenderName,
                                  Logger logger,
                                  ExcerptAppender appender,
                                  Supplier<T> dataProducer) {
            super(allocateSeq, appenderName, logger, appender, dataProducer);
        }

        @Override
        protected void append0(@Nonnull T t) {
            appender.writeDocument(t);
        }

    }

    @Immutable
    @NotThreadSafe
    public static final class ChronicleDocumentReader<T extends Marshallable> extends AbstractChronicleReader<T> {

        private final Supplier<T> marshallableSupplier;

        ChronicleDocumentReader(long allocateSeq,
                                String readerName,
                                FileCycle fileCycle,
                                ReaderParams param,
                                Logger logger,
                                ExcerptTailer tailer,
                                Consumer<T> dataConsumer,
                                Supplier<T> marshallableSupplier) {
            super(allocateSeq, readerName, fileCycle, param, logger, tailer, dataConsumer);
            this.marshallableSupplier = marshallableSupplier;
        }

        @Override
        protected T next0() {
            final T t = marshallableSupplier.get();
            return tailer.readDocument(t) ? t : null;
        }

    }

}
