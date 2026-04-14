package io.flatf.common.concurrent.disruptor.base;

import com.lmax.disruptor.EventHandler;
import io.flatf.common.functional.Processor;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import org.slf4j.Logger;

/**
 * 事件处理器的包装
 *
 * @author yellow013
 */
public final class EventHandlerWrapper<E> implements EventHandler<E> {

    private static final Logger LOG = Log4j2LoggerFactory.getLogger(EventHandlerWrapper.class);

    private final Processor<E> processor;

    private final Logger logger;

    private final boolean crashOnFailure;

    private EventHandlerWrapper(Processor<E> processor, Logger logger, boolean crashOnFailure) {
        this.processor = processor;
        this.logger = logger == null ? LOG : logger;
        this.crashOnFailure = crashOnFailure;
    }

    @Override
    public void onEvent(E event, long sequence, boolean endOfBatch) throws Exception {
        try {
            processor.process(event);
        } catch (Exception e) {
            logger.error("EventHandler process event -> {}, sequence==[{}], endOfBatch==[{}], Processor -> {}, Throw exception -> [{}]",
                    event, sequence, endOfBatch, processor.getClass().getSimpleName(), e.getMessage(), e);
            if (crashOnFailure)
                throw e;
        }
    }

    public static <E> Builder<E> builder() {
        return new Builder<>();
    }


    /**
     * @param <E>
     */
    public static class Builder<E> {

        private Logger logger;
        private boolean crashOnFailure = false;

        public Builder<E> logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder<E> crashOnFailure(boolean crashOnFailure) {
            this.crashOnFailure = crashOnFailure;
            return this;
        }

        public EventHandlerWrapper<E> build(Processor<E> processor) {
            return new EventHandlerWrapper<>(processor, logger, crashOnFailure);
        }

    }


}
