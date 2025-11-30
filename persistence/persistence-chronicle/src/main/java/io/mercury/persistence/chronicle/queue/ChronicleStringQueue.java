package io.mercury.persistence.chronicle.queue;

import io.mercury.common.number.Randoms;
import io.mercury.common.sequence.EpochSequence;
import io.mercury.common.thread.Sleep;
import io.mercury.persistence.chronicle.queue.ChronicleStringQueue.ChronicleStringAppender;
import io.mercury.persistence.chronicle.queue.ChronicleStringQueue.ChronicleStringReader;
import io.mercury.persistence.chronicle.queue.params.ReaderParams;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.slf4j.Logger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Immutable
public class ChronicleStringQueue
        extends AbstractChronicleQueue<String, ChronicleStringAppender, ChronicleStringReader> {

    private ChronicleStringQueue(StringQueueBuilder builder) {
        super(builder);
    }

    public static StringQueueBuilder newBuilder() {
        return new StringQueueBuilder();
    }

    @Override
    protected ChronicleStringReader createReader(@Nonnull String readerName,
                                                 @Nonnull ReaderParams readerParam,
                                                 @Nonnull Logger logger,
                                                 @Nonnull Consumer<String> dataConsumer)
            throws IllegalStateException {
        return new ChronicleStringReader(EpochSequence.allocate(), readerName, fileCycle(), readerParam, logger,
                internalQueue.createTailer(), dataConsumer);
    }

    @Override
    protected ChronicleStringAppender acquireAppender(@Nonnull String appenderName,
                                                      @Nonnull Logger logger,
                                                      @CheckForNull Supplier<String> dataProducer)
            throws IllegalStateException {
        return new ChronicleStringAppender(EpochSequence.allocate(), appenderName, logger,
                internalQueue.createAppender(), dataProducer);
    }

    /**
     * @author yellow013
     */
    public static final class StringQueueBuilder extends BaseQueueBuilder<StringQueueBuilder> {

        private StringQueueBuilder() {
        }

        public ChronicleStringQueue build() {
            return new ChronicleStringQueue(this);
        }

        @Override
        protected StringQueueBuilder self() {
            return this;
        }

    }

    @Immutable
    @NotThreadSafe
    public static final class ChronicleStringAppender extends AbstractChronicleAppender<String> {

        ChronicleStringAppender(long allocateSeq,
                                String appenderName,
                                Logger logger,
                                ExcerptAppender appender,
                                Supplier<String> dataProducer) {
            super(allocateSeq, appenderName, logger, appender, dataProducer);
        }

        @Override
        protected void append0(@Nonnull String t) {
            appender.writeText(t);
        }

    }

    @Immutable
    @NotThreadSafe
    public static final class ChronicleStringReader extends AbstractChronicleReader<String> {

        ChronicleStringReader(long allocateSeq,
                              String readerName,
                              FileCycle fileCycle,
                              ReaderParams params,
                              Logger logger,
                              ExcerptTailer tailer,
                              Consumer<String> dataConsumer) {
            super(allocateSeq, readerName, fileCycle, params, logger, tailer, dataConsumer);
        }

        @Override
        protected String next0() {
            return tailer.readText();
        }

    }

    public static void main(String[] args) {
        try (ChronicleStringQueue queue = ChronicleStringQueue.newBuilder()
                .rootPath("d:/dump")

                .fileCycle(FileCycle.FAST_DAILY).build()) {
            ChronicleStringAppender appender = queue.acquireAppender();
            ChronicleStringReader reader = queue.createReader(System.out::println);
            new Thread(() -> {
                for (; ; ) {
                    try {
                        appender.append(String.valueOf(Randoms.nextLong()));
                        Sleep.millis(100);
                    } catch (Exception ignored) {
                    }
                }
            }).start();
            reader.runWithNewThread();
        }
    }

}
