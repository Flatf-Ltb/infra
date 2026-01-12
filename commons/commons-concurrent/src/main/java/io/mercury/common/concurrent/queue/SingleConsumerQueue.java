package io.mercury.common.concurrent.queue;

import io.mercury.common.collections.queue.Queue;
import io.mercury.common.functional.Processor;
import io.mercury.common.lang.Validator;
import io.mercury.common.thread.RunnableComponent;

import java.time.LocalDateTime;

import static io.mercury.common.datetime.pattern.impl.DateTimePattern.YYYYMMDD_L_HHMMSSSSS;

/**
 * @param <E> Single Consumer Queue base implements
 * @author yellow013
 */
public abstract class SingleConsumerQueue<E> extends RunnableComponent implements Queue<E> {

    /**
     * Processor Function
     */
    protected final Processor<E> processor;

    protected SingleConsumerQueue(Processor<E> processor) {
        Validator.nonNull(processor, "processor");
        this.processor = processor;
        this.name = "queue-" + "[" + YYYYMMDD_L_HHMMSSSSS.fmt(LocalDateTime.now()) + "]";
    }

    @Override
    public String getQueueName() {
        return name;
    }

}
