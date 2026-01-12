package io.mercury.common.concurrent.disruptor.base;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.mercury.common.log4j2.Log4j2LoggerFactory;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static io.mercury.common.collections.CollectionUtil.toArray;
import static io.mercury.common.collections.MutableLists.newFastList;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNullElse;

/**
 * [事件处理器] 管理器
 *
 * @param <E> 事件类型
 */
public final class HandlerManager<E> {

    private static final Logger log = Log4j2LoggerFactory.getLogger(HandlerManager.class);

    private final HandleType type;

    private final ExceptionHandler<E> exceptionHandler;

    private final List<EventHandler<E>> eventHandlers;

    private final MutableIntObjectMap<List<EventHandler<E>>> sortedEventHandlers;

    private HandlerManager(@Nonnull HandleType type,
                           @Nullable ExceptionHandler<E> exceptionHandler,
                           @Nullable List<EventHandler<E>> eventHandlers,
                           @Nullable MutableIntObjectMap<List<EventHandler<E>>> sortedEventHandlers) {
        this.type = type;
        this.exceptionHandler = exceptionHandler;
        this.eventHandlers = eventHandlers;
        this.sortedEventHandlers = sortedEventHandlers;
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
        switch (type) {
            case SINGLE, PIPELINE -> setPipelineMode(disruptor);
            case BROADCAST -> setBroadcastMode(disruptor);
        }
    }

    /**
     * 设置管道模式
     *
     * @param disruptor Disruptor<E>
     */
    private void setPipelineMode(Disruptor<E> disruptor) {
        if (eventHandlers != null) {
            if (eventHandlers.size() > 1) {
                var handlerGroup = disruptor.handleEventsWith(eventHandlers.getFirst());
                for (int i = 1; 1 < eventHandlers.size(); i++) {
                    handlerGroup.then(eventHandlers.get(i));
                }
            } else {
                // With set single event
                disruptor.handleEventsWith(eventHandlers.getFirst());
            }
        } else {
            throw new IllegalStateException("List<EventHandler> is null");
        }
    }

    /**
     * 设置广播模式
     *
     * @param disruptor Disruptor<E>
     */
    @SuppressWarnings("unchecked")
    private void setBroadcastMode(Disruptor<E> disruptor) {
        if (eventHandlers != null)
            disruptor.handleEventsWith(toArray(eventHandlers, EventHandler[]::new));
        else
            throw new IllegalStateException("List<EventHandler> is null");

    }


    public static <E> HandlerManager<E> single(@Nonnull EventHandler<E> handler) {
        return new EventHandlerWizard<E>(HandleType.SINGLE)
                .add(handler)
                .build();
    }

    public static <E> HandlerManager<E> single(@Nonnull EventHandler<E> handler,
                                               @Nonnull ExceptionHandler<E> exceptionHandler) {
        return new EventHandlerWizard<E>(HandleType.SINGLE)
                .add(handler)
                .exceptionHandler(exceptionHandler)
                .build();
    }

    public static <E> EventHandlerWizard<E> pipeline() {
        return new EventHandlerWizard<>(HandleType.PIPELINE);
    }

    @SafeVarargs
    public static <E> EventHandlerWizard<E> pipelineWith(@Nonnull EventHandler<E>... handlers) {
        return new EventHandlerWizard<E>(HandleType.PIPELINE)
                .add(handlers);
    }

    public static <E> EventHandlerWizard<E> broadcast() {
        return new EventHandlerWizard<>(HandleType.BROADCAST);
    }

    @SafeVarargs
    public static <E> EventHandlerWizard<E> broadcastTo(@Nonnull EventHandler<E>... handlers) {
        return new EventHandlerWizard<E>(HandleType.BROADCAST)
                .add(handlers);
    }

    private abstract static class Wizard<E, W extends Wizard<E, W>> {

        protected final HandleType type;

        protected ExceptionHandler<E> exceptionHandler;

        private Wizard(HandleType type) {
            this.type = type;
        }

        public W exceptionHandler(ExceptionHandler<E> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return returnThis();
        }

        public abstract W returnThis();

        public abstract HandlerManager<E> build();

    }

    public static class EventHandlerWizard<E> extends Wizard<E, EventHandlerWizard<E>> {

        private final MutableList<EventHandler<E>> eventHandlers = newFastList();

        private EventHandlerWizard(HandleType type) {
            super(type);
        }

        @SafeVarargs
        public final EventHandlerWizard<E> add(EventHandler<E>... handlers) {
            this.eventHandlers.addAll(asList(handlers));
            return this;
        }

        @Override
        public EventHandlerWizard<E> returnThis() {
            return this;
        }

        @Override
        public HandlerManager<E> build() {
            return new HandlerManager<>(type, exceptionHandler, eventHandlers, null);
        }

    }

    private enum HandleType {
        SINGLE,
        PIPELINE,
        BROADCAST
    }

}
