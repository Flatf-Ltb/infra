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

    private EventHandlerWrapper(Processor<E> processor, Logger log, boolean canCrash) {
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

    public static <E> Builder<E> newBuilder() {
        return new Builder<>();
    }

    /**
     * @param <E>
     */
    public static class Builder<E> {

        private Logger logger = null;
        private boolean canCrash = false;

        public Builder<E> logger(Logger logger) {
            this.logger = logger;
            return this;

        }

        public Builder<E> canCrash(boolean canCrash) {
            this.canCrash = canCrash;
            return this;
        }

        public EventHandlerWrapper<E> build(Processor<E> processor) {
            return new EventHandlerWrapper<>(processor, logger, canCrash);
        }

    }


}
