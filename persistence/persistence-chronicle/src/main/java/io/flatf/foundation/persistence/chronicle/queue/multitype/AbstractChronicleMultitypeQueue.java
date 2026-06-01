package io.flatf.foundation.persistence.chronicle.queue.multitype;

import io.flatf.foundation.common.codec.Envelope;
import io.flatf.foundation.persistence.chronicle.queue.AbstractChronicleQueue;
import io.flatf.foundation.persistence.chronicle.queue.AbstractChronicleReader;

import javax.annotation.concurrent.Immutable;

/**
 * @param <E>
 * @param <T>
 * @param <AT>
 * @param <RT>
 * @author yellow013
 */
@Immutable
public abstract class AbstractChronicleMultitypeQueue<
        // 信封类型
        E extends Envelope,
        // 写入读取类型
        T,
        // 追加器类型
        AT extends AbstractChronicleMultitypeAppender<E, T>,
        // 读取器类型
        RT extends AbstractChronicleReader<T>>
        extends AbstractChronicleQueue<T, AT, RT> {

    protected AbstractChronicleMultitypeQueue(BaseQueueBuilder<?> builder) {
        super(builder);
    }

}
