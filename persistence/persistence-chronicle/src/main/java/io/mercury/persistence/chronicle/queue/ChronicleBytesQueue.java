package io.mercury.persistence.chronicle.queue;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;

import io.mercury.common.number.Randoms;
import io.mercury.common.sequence.EpochSequence;
import io.mercury.common.thread.Sleep;
import io.mercury.persistence.chronicle.queue.ChronicleBytesQueue.ChronicleBytesAppender;
import io.mercury.persistence.chronicle.queue.ChronicleBytesQueue.ChronicleBytesReader;
import io.mercury.persistence.chronicle.queue.params.ReaderParams;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;

@Immutable
public class ChronicleBytesQueue
        extends AbstractChronicleQueue<ByteBuffer, ByteBuffer, ChronicleBytesAppender, ChronicleBytesReader> {

    private final int bufferSize;
    private final boolean useDirectMemory;

    private ChronicleBytesQueue(BytesQueueBuilder builder) {
        super(builder);
        this.bufferSize = builder.bufferSize;
        this.useDirectMemory = builder.useDirectMemory;
    }

    /**
     * @return BytesQueueBuilder
     */
    public static BytesQueueBuilder newBuilder() {
        return new BytesQueueBuilder();
    }

    @Override
    protected ChronicleBytesReader createReader(@Nonnull String readerName,
                                                @Nonnull ReaderParams readerParam,
                                                @Nonnull Logger logger,
                                                @Nonnull Consumer<ByteBuffer> dataConsumer)
            throws IllegalStateException {
        return new ChronicleBytesReader(EpochSequence.allocate(), readerName, fileCycle(), readerParam, logger,
                bufferSize, useDirectMemory, internalQueue.createTailer(), dataConsumer);
    }

    @Override
    protected ChronicleBytesAppender acquireAppender(@Nonnull String appenderName,
                                                     @Nonnull Logger logger,
                                                     @CheckForNull Supplier<ByteBuffer> dataProducer)
            throws IllegalStateException {
        return new ChronicleBytesAppender(EpochSequence.allocate(), appenderName, logger,
                internalQueue.createAppender(), dataProducer);
    }

    /**
     * @author yellow013
     */
    public static final class BytesQueueBuilder extends AbstractQueueBuilder<BytesQueueBuilder> {

        private int bufferSize = 512;
        private boolean useDirectMemory = false;

        private BytesQueueBuilder() {
        }

        public ChronicleBytesQueue build() {
            return new ChronicleBytesQueue(this);
        }

        /**
         * if set size less than 512, use default size the 512
         *
         * @param bufferSize int
         * @return BytesQueueBuilder
         */
        public BytesQueueBuilder bufferSize(int bufferSize) {
            this.bufferSize = Math.max(bufferSize, 512);
            return this;
        }

        public BytesQueueBuilder useDirectMemory(boolean useDirectMemory) {
            this.useDirectMemory = useDirectMemory;
            return this;
        }

        @Override
        protected BytesQueueBuilder self() {
            return this;
        }

    }

    /**
     * @author yellow013
     */
    @Immutable
    @NotThreadSafe
    public static final class ChronicleBytesAppender extends AbstractChronicleAppender<ByteBuffer> {

        ChronicleBytesAppender(long allocateSeq,
                               String appenderName,
                               Logger logger,
                               ExcerptAppender appender,
                               Supplier<ByteBuffer> dataProducer) {
            super(allocateSeq, appenderName, logger, appender, dataProducer);
        }

        @Override
        protected void append0(@Nonnull ByteBuffer t) {
            // use heap memory or direct by the byteBuffer
            appender.writeBytes(BytesStore.wrap(t));
        }

    }

    /**
     * @author yellow013
     */
    @Immutable
    @NotThreadSafe
    public static final class ChronicleBytesReader extends AbstractChronicleReader<ByteBuffer> {

        private final int bufferSize;
        private final boolean useDirectMemory;

        ChronicleBytesReader(long allocateSeq,
                             String readerName,
                             FileCycle fileCycle,
                             ReaderParams params,
                             Logger logger,
                             int bufferSize,
                             boolean useDirectMemory,
                             ExcerptTailer tailer,
                             Consumer<ByteBuffer> dataConsumer) {
            super(allocateSeq, readerName, fileCycle, params, logger, tailer, dataConsumer);
            this.bufferSize = bufferSize;
            this.useDirectMemory = useDirectMemory;
        }

        @Override
        protected ByteBuffer next0() {
            Bytes<ByteBuffer> bytes;
            if (useDirectMemory)
                // use direct memory
                bytes = Bytes.elasticByteBuffer(bufferSize);
            else
                // use heap memory
                bytes = Bytes.elasticHeapByteBuffer(bufferSize);
            tailer.readBytes(bytes);
            if (bytes.isEmpty())
                return null;
            logger.debug("ChronicleBytesReader.next0() -> {}", bytes.toDebugString());
            return bytes.underlyingObject();
        }

    }

    public static void main(String[] args) {
        ChronicleBytesQueue queue = ChronicleBytesQueue.newBuilder().folder("byte-test").bufferSize(512)
                .fileCycle(FileCycle.FAST_DAILY).build();
        ChronicleBytesAppender writer = queue.acquireAppender();
        ChronicleBytesReader reader = queue.createReader(next -> System.out.println(new String(next.array())));
        new Thread(() -> {
            ByteBuffer buffer = ByteBuffer.allocate(512);
            for (; ; ) {
                try {
                    writer.append(buffer.put(String.valueOf(Randoms.nextLong()).getBytes()));
                    buffer.clear();
                    Sleep.millis(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        reader.runWithNewThread();
    }

}
