package io.flatf.foundation.common.concurrent.queue;

import io.flatf.foundation.common.collections.queue.Queue;
import io.flatf.foundation.common.functional.Processor;
import io.flatf.foundation.common.lang.Validator;
import io.flatf.foundation.common.thread.RunnableComponent;

/**
 * @param <E> Single Consumer Queue base implements
 * @author yellow013
 */
public abstract class SingleConsumerQueue<E> extends RunnableComponent implements Queue<E> {

    /**
     * Processor Function
     */
    protected final Processor<E> processor;

    protected SingleConsumerQueue(String name, Processor<E> processor) {
        super(name);
        Validator.nonNull(processor, "processor");
        this.processor = processor;
    }

    @Override
    public String getQueueName() {
        return name;
    }

}
