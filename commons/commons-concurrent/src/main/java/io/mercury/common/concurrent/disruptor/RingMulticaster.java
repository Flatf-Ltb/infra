package io.mercury.common.concurrent.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import io.mercury.common.collections.MutableLists;
import io.mercury.common.concurrent.disruptor.base.CommonStrategy;
import io.mercury.common.concurrent.disruptor.base.EventHandlerWrapper;
import io.mercury.common.concurrent.disruptor.base.ReflectionEventFactory;
import io.mercury.common.concurrent.disruptor.base.RingComponent;
import io.mercury.common.functional.Processor;
import io.mercury.common.lang.ThrowsUtil;
import io.mercury.common.log4j2.Log4j2LoggerFactory;
import io.mercury.common.util.StringSupport;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.time.LocalDateTime;
import java.util.List;

import static io.mercury.common.collections.CollectionUtil.toArray;
import static io.mercury.common.datetime.pattern.impl.DateTimePattern.YYYYMMDD_L_HHMMSSSSS;
import static io.mercury.common.lang.Validator.nonNull;
import static io.mercury.common.lang.Validator.requiredLength;
import static io.mercury.common.sys.CurrentRuntime.availableProcessors;

/**
 * @param <E> 进行处理的类型
 * @param <I> 发布的数据类型
 * @author yellow013
 */
public final class RingMulticaster<E, I> extends RingComponent<E, I> {

    private static final Logger log = Log4j2LoggerFactory.getLogger(RingMulticaster.class);

    /**
     * @param name       String
     * @param size       int
     * @param mode       StartMode
     * @param type       ProducerType
     * @param strategy   WaitStrategy
     * @param factory    EventFactory<E>
     * @param translator EventTranslatorOneArg<E, I>
     * @param handlers   List<EventHandler<E>>
     */
    @SuppressWarnings("unchecked")
    private RingMulticaster(String name, int size, StartMode mode, ProducerType type,
                            WaitStrategy strategy, EventFactory<E> factory,
                            EventTranslatorOneArg<E, I> translator,
                            List<EventHandler<E>> handlers) {
        super(name, size, type, strategy, factory, translator);
        requiredLength(handlers, 1, "handlers");
        // 将处理器添加进Disruptor中, 各个处理器进行并行处理
        disruptor.handleEventsWith(toArray(handlers, EventHandler[]::new));
        log.info("Initialized RingMulticaster -> {}, size -> {}, ProducerType -> {}, " +
                        "WaitStrategy -> {}, StartMode -> {}, EventHandler count -> {}",
                this.name, size, type, strategy, mode, handlers.size());
        startWith(mode);
    }

    // **************** 单生产者广播器 ****************//
    public static <E, I> Builder<E, I> singleProducer(@Nonnull Class<E> eventType,
                                                      @Nonnull RingEventPublisher<E, I> publisher) {
        return singleProducer( // 使用反射EventFactory
                ReflectionEventFactory.newFactory(eventType, log), publisher);
    }

    public static <E, I> Builder<E, I> singleProducer(@Nonnull Class<E> eventType,
                                                      @Nonnull EventTranslatorOneArg<E, I> translator) {
        return singleProducer( // 使用反射EventFactory
                ReflectionEventFactory.newFactory(eventType, log), translator);
    }

    public static <E, I> Builder<E, I> singleProducer(@Nonnull EventFactory<E> eventFactory,
                                                      @Nonnull RingEventPublisher<E, I> publisher) {
        return singleProducer(eventFactory,
                // EventTranslator实现函数, 负责调用处理In对象到Event对象之间的转换
                (event, sequence, in) -> publisher.accept(event, in));
    }

    public static <E, I> Builder<E, I> singleProducer(@Nonnull EventFactory<E> eventFactory,
                                                      @Nonnull EventTranslatorOneArg<E, I> translator) {
        return new Builder<>(ProducerType.SINGLE, eventFactory, translator);
    }

    // **************** 多生产者广播器 ****************//
    public static <E, I> Builder<E, I> multiProducer(@Nonnull Class<E> eventType,
                                                     @Nonnull RingEventPublisher<E, I> publisher) {
        return multiProducer( // 使用反射EventFactory
                ReflectionEventFactory.newFactory(eventType, log), publisher);
    }

    public static <E, I> Builder<E, I> multiProducer(@Nonnull Class<E> eventType,
                                                     @Nonnull EventTranslatorOneArg<E, I> translator) {
        return multiProducer( // 使用反射EventFactory
                ReflectionEventFactory.newFactory(eventType, log), translator);
    }

    public static <E, I> Builder<E, I> multiProducer(@Nonnull EventFactory<E> eventFactory,
                                                     @Nonnull RingEventPublisher<E, I> publisher) {
        return multiProducer(eventFactory,
                // EventTranslator实现函数, 负责调用处理In对象到Event对象之间的转换
                (event, sequence, in) -> publisher.accept(event, in));
    }

    public static <E, I> Builder<E, I> multiProducer(@Nonnull EventFactory<E> eventFactory,
                                                     @Nonnull EventTranslatorOneArg<E, I> translator) {
        return new Builder<>(ProducerType.MULTI, eventFactory, translator);
    }


    public static class Builder<E, I> {

        private String name;
        private int size = 64;
        private StartMode mode = StartMode.auto();
        private WaitStrategy waitStrategy;
        private final ProducerType producerType;
        private final EventFactory<E> eventFactory;
        private final EventTranslatorOneArg<E, I> eventTranslator;
        private final List<EventHandler<E>> handlers = MutableLists.newFastList();

        private Builder(ProducerType producerType,
                        EventFactory<E> eventFactory,
                        EventTranslatorOneArg<E, I> translator) {
            this.producerType = producerType;
            this.eventFactory = eventFactory;
            this.eventTranslator = translator;
        }

        public Builder<E, I> addProcessor(@Nonnull Processor<E> processor) {
            nonNull(processor, "processor");
            return addHandler(
                    // 将Processor实现加载到HandlerWrapper中
                    new EventHandlerWrapper<>(processor, log));
        }

        public Builder<E, I> addHandler(@Nonnull EventHandler<E> handler) {
            nonNull(handler, "handler");
            this.handlers.add(handler);
            return this;
        }

        public Builder<E, I> setName(String name) {
            this.name = name;
            return this;
        }

        public Builder<E, I> setWaitStrategy(CommonStrategy waitStrategy) {
            return setWaitStrategy(waitStrategy.get());
        }

        public Builder<E, I> setWaitStrategy(WaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
            return this;
        }

        public Builder<E, I> setStartMode(StartMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder<E, I> size(int size) {
            this.size = size;
            return this;
        }

        public RingMulticaster<E, I> create() {
            if (handlers.isEmpty())
                ThrowsUtil.throwsIllegalArgument("handlers");
            if (waitStrategy == null)
                waitStrategy = handlers.size() > availableProcessors() ? CommonStrategy.Sleeping.get() : CommonStrategy.Yielding.get();
            if (StringSupport.isNullOrEmpty(name))
                name = "RingMulticaster-" + YYYYMMDD_L_HHMMSSSSS.fmt(LocalDateTime.now());
            return new RingMulticaster<>(name, size, mode, producerType, waitStrategy, eventFactory,
                    eventTranslator, handlers);
        }

    }

}
