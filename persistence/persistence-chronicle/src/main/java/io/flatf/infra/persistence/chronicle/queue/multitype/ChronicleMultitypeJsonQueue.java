package io.flatf.infra.persistence.chronicle.queue.multitype;

import io.flatf.common.codec.Envelope;
import io.flatf.common.sequence.EpochSequence;
import io.flatf.infra.persistence.chronicle.queue.AbstractChronicleReader;
import io.flatf.infra.persistence.chronicle.queue.FileCycle;
import io.flatf.infra.persistence.chronicle.queue.multitype.ChronicleMultitypeJsonQueue.ChronicleMultitypeJsonAppender;
import io.flatf.infra.persistence.chronicle.queue.multitype.ChronicleMultitypeJsonQueue.ChronicleMultitypeJsonReader;
import io.flatf.infra.persistence.chronicle.queue.params.ReaderParams;
import io.flatf.infra.serialization.json.JsonMsg;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Immutable
public class ChronicleMultitypeJsonQueue<E extends Envelope> extends
        AbstractChronicleMultitypeQueue<
                // 信封类型
                E,
                // 写入读取类型
                String,
                // 追加器类型
                ChronicleMultitypeJsonAppender<E>,
                // 读取器类型
                ChronicleMultitypeJsonReader> {

    private ChronicleMultitypeJsonQueue(MultitypeJsonQueueBuilder<E> builder) {
        super(builder);
    }

    public static <E extends Envelope> MultitypeJsonQueueBuilder<E> newBuilder(Class<E> envelopeType) {
        return new MultitypeJsonQueueBuilder<>();
    }

    @Override
    protected ChronicleMultitypeJsonReader createReader(@Nonnull String readerName,
                                                        @Nonnull ReaderParams params,
                                                        @Nonnull Logger logger,
                                                        @Nonnull Consumer<String> dataConsumer)
            throws IllegalStateException {
        return new ChronicleMultitypeJsonReader(EpochSequence.allocate(), readerName,
                fileCycle(), params, logger, internalQueue.createTailer(),
                dataConsumer);
    }

    @Override
    protected ChronicleMultitypeJsonAppender<E> acquireAppender(@Nonnull String appenderName,
                                                                @Nonnull Logger logger,
                                                                @Nullable Supplier<String> dataProducer)
            throws IllegalStateException {
        return new ChronicleMultitypeJsonAppender<>(EpochSequence.allocate(), appenderName, logger,
                internalQueue.createAppender(), dataProducer);
    }

    /**
     * @author yellow013
     */
    public static final class MultitypeJsonQueueBuilder<E extends Envelope> extends BaseQueueBuilder<MultitypeJsonQueueBuilder<E>> {

        private MultitypeJsonQueueBuilder() {
        }

        public ChronicleMultitypeJsonQueue<E> build() {
            return new ChronicleMultitypeJsonQueue<>(this);
        }

        @Override
        protected MultitypeJsonQueueBuilder<E> self() {
            return this;
        }

    }

    @Immutable
    @NotThreadSafe
    public static final class ChronicleMultitypeJsonAppender<E extends Envelope>
            extends AbstractChronicleMultitypeAppender<E, String> {

        ChronicleMultitypeJsonAppender(long allocateSeq,
                                       String name,
                                       Logger logger,
                                       ExcerptAppender appender,
                                       Supplier<String> dataProducer) {
            super(allocateSeq, name, logger, appender, dataProducer);
        }

        // 內建JsonMsg对象
        private final JsonMsg jsonMsg = new JsonMsg();

        @Override
        protected void append0(@Nullable E envelope, @Nonnull String t) {
            if (envelope != null) {
                appender.writeText(
                        // 设置信封
                        jsonMsg.setEnvelope(envelope.getCode())
                                // 设置内容
                                .setContent(t)
                                // JsonMsg序列化为JSON
                                .toJson());
            } else {
                logger.warn("envelope == null, message discarded -> {}", t);
            }
        }
    }

    @Immutable
    @NotThreadSafe
    public static final class ChronicleMultitypeJsonReader extends AbstractChronicleReader<String> {

        ChronicleMultitypeJsonReader(long allocateSeq,
                                     String name,
                                     FileCycle fileCycle,
                                     ReaderParams params,
                                     Logger logger,
                                     ExcerptTailer tailer,
                                     Consumer<String> dataConsumer) {
            super(allocateSeq, name, fileCycle, params, logger, tailer, dataConsumer);
        }

        @Override
        protected String next0() {
            return tailer.readText();
        }

    }

}
