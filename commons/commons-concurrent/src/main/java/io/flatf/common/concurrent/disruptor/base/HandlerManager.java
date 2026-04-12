package io.flatf.common.concurrent.disruptor.base;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import org.eclipse.collections.api.list.MutableList;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static io.flatf.common.collections.MutableLists.newFastList;
import static java.util.Objects.requireNonNullElse;

/**
 * [事件处理器] 管理器
 *
 * @param <E> 事件类型
 */
public final class HandlerManager<E> {

    private static final Logger log = Log4j2LoggerFactory.getLogger(HandlerManager.class);

    private final ExceptionHandler<E> exceptionHandler;

    private final MutableList<EventHandler<E>[]> eventHandlersList;

    private HandlerManager(@Nonnull MutableList<EventHandler<E>[]> eventHandlersList,
                           @Nullable ExceptionHandler<E> exceptionHandler) {
        this.eventHandlersList = eventHandlersList;
        this.exceptionHandler = exceptionHandler;
    }

    private static class ExceptionLogger<E> implements ExceptionHandler<E> {

        @Override
        public void handleEventException(Throwable ex, long sequence, E event) {
            log.error("handleEventException -> event==[{}], sequence==[{}], exception message==[{}]",
                    event, sequence, ex.getMessage(), ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            log.error("handleOnStartException -> {}", ex.getMessage(), ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            log.error("handleOnShutdownException -> {}", ex.getMessage(), ex);
        }
    }

    public void deploy(Disruptor<E> disruptor) {
        disruptor.setDefaultExceptionHandler(requireNonNullElse(exceptionHandler, new ExceptionLogger<>()));
        if (eventHandlersList.size() > 1) {
            var handlerGroup = disruptor.handleEventsWith(eventHandlersList.getFirst());
            for (int i = 1; 1 < eventHandlersList.size(); i++)
                handlerGroup.then(eventHandlersList.get(i));
        } else {
            // With set single event
            disruptor.handleEventsWith(eventHandlersList.getFirst());
        }
    }


    @SafeVarargs
    public static <E> HandlerManager.EventHandlerWizard<E> firstWith(@Nonnull EventHandler<E>... handlers) {
        return new EventHandlerWizard<E>()
                .then(handlers);
    }

    public static class EventHandlerWizard<E> {

        protected ExceptionHandler<E> exceptionHandler;
        private final MutableList<EventHandler<E>[]> eventHandlers = newFastList();

        private EventHandlerWizard() {
        }

        @SafeVarargs
        public final EventHandlerWizard<E> then(EventHandler<E>... handlers) {
            this.eventHandlers.add(handlers);
            return this;
        }

        public EventHandlerWizard<E> exceptionHandler(ExceptionHandler<E> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        public HandlerManager<E> build() {
            return new HandlerManager<>(eventHandlers, exceptionHandler);
        }

    }


}
