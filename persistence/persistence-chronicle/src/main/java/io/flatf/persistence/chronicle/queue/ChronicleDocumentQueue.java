package io.flatf.persistence.chronicle.queue;

import io.flatf.common.lang.Validator;
import io.flatf.common.sequence.EpochSequence;
import io.flatf.persistence.chronicle.queue.ChronicleDocumentQueue.ChronicleDocumentAppender;
import io.flatf.persistence.chronicle.queue.ChronicleDocumentQueue.ChronicleDocumentReader;
import io.flatf.persistence.chronicle.queue.params.ReaderParams;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.Marshallable;
import org.slf4j.Logger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Immutable
public class ChronicleDocumentQueue<T extends Marshallable>
        extends AbstractChronicleQueue<T, ChronicleDocumentAppender<T>, ChronicleDocumentReader<T>> {

    private final Supplier<T> marshallableSupplier;

    private ChronicleDocumentQueue(DocumentQueueBuilder<T> builder) {
        super(builder);
        Validator.nonNull(builder.marshallableSupplier, "builder.marshallableSupplier");
        this.marshallableSupplier = builder.marshallableSupplier;
    }

    public static <T extends Marshallable> DocumentQueueBuilder<T> newBuilder(Class<T> saveType) {
        return new DocumentQueueBuilder<>();
    }

    @Override
    protected ChronicleDocumentReader<T> createReader(@Nonnull String name,
                                                      @Nonnull ReaderParams param,
                                                      @Nonnull Logger logger,
                                                      @Nonnull Consumer<T> consumer)
            throws IllegalStateException {
        return new ChronicleDocumentReader<>(EpochSequence.allocate(), name, fileCycle(), param, logger,
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
                                  String name,
                                  Logger logger,
                                  ExcerptAppender appender,
                                  Supplier<T> dataProducer) {
            super(allocateSeq, name, logger, appender, dataProducer);
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
                                String name,
                                FileCycle fileCycle,
                                ReaderParams param,
                                Logger logger,
                                ExcerptTailer tailer,
                                Consumer<T> dataConsumer,
                                Supplier<T> marshallableSupplier) {
            super(allocateSeq, name, fileCycle, param, logger, tailer, dataConsumer);
            this.marshallableSupplier = marshallableSupplier;
        }

        @Override
        protected T next0() {
            final T t = marshallableSupplier.get();
            return tailer.readDocument(t) ? t : null;
        }

    }

}
