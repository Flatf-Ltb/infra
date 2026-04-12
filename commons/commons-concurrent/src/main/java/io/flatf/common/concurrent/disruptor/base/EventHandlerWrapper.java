package io.flatf.common.concurrent.disruptor.base;

import com.lmax.disruptor.EventHandler;
import io.flatf.common.functional.Processor;
import org.slf4j.Logger;

/**
 * 事件处理器的包装
 *
 * @author yellow013
 */
public final class EventHandlerWrapper<E> implements EventHandler<E> {

    private final Processor<E> processor;

    private final Logger log;

    private final boolean canCrash;

    public EventHandlerWrapper(Processor<E> processor, Logger log) {
        this(processor, log, true);
    }

    public EventHandlerWrapper(Processor<E> processor, Logger log, boolean canCrash) {
        this.processor = processor;
        this.log = log;
        this.canCrash = canCrash;
    }

    @Override
    public void onEvent(E event, long sequence, boolean endOfBatch) throws Exception {
        try {
            processor.process(event);
        } catch (Exception e) {
            log.error("EventHandler process event -> {}, sequence==[{}], endOfBatch==[{}], Throw exception -> [{}]",
                    event, sequence, endOfBatch, e.getMessage(), e);
            if (canCrash)
                throw e;
        }
    }

}
