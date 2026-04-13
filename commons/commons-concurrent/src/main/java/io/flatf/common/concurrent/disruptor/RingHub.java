package io.flatf.common.concurrent.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.flatf.common.concurrent.disruptor.base.EventPublisherArg1;
import io.flatf.common.concurrent.disruptor.base.EventPublisherArg2;
import io.flatf.common.concurrent.disruptor.base.EventPublisherArg3;
import io.flatf.common.concurrent.disruptor.base.ReflectionEventFactory;
import io.flatf.common.thread.RunnableComponent;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.LocalDateTime;

import static com.lmax.disruptor.dsl.ProducerType.MULTI;
import static com.lmax.disruptor.dsl.ProducerType.SINGLE;
import static io.flatf.common.concurrent.disruptor.SimpleWaitStrategy.YIELDING;
import static io.flatf.common.datetime.pattern.impl.DateTimePattern.YYMMDD_L_HHMMSSSSS;
import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;
import static io.flatf.common.thread.ThreadFactoryImpl.ofPlatform;
import static io.flatf.common.thread.ThreadPriority.MAX;
import static io.flatf.common.util.BitOperator.minPow2;
import static java.util.Objects.requireNonNullElse;

/**
 * @param <E>
 * @author yellow013
 * <p>
 * 扩展多写和单写 [DONE]
 */
public final class RingHub<E> extends RunnableComponent {

    private static final Logger log = getLogger(RingHub.class);

    private final Disruptor<E> disruptor;

    private final RingBuffer<E> ringBuffer;

    private final boolean isMultiProducer;

    

    private RingHub(@Nullable String name, int size,
                    @Nonnull StartMode mode, @Nonnull ProducerType type,
                    @Nonnull EventFactory<E> factory,
                    @Nonnull WaitStrategy strategy,
                    @Nonnull HandlerGraph<E> graph) {
        this.name = requireNonNullElse(name,
                "RingHub-[" + YYMMDD_L_HHMMSSSSS.fmt(LocalDateTime.now()) + "]");
        this.disruptor = new Disruptor<>(
                // EventFactory, 队列容量
                factory, adjustSize(size),
                // ThreadFactory
                ofPlatform(this.name + "-worker").priority(MAX).build(),
                // 生产者类型, Waiting策略
                type, strategy);
        graph.deploy(this.disruptor);
        this.ringBuffer = this.disruptor.getRingBuffer();
        this.isMultiProducer = (type == MULTI);
        startWith(mode);
    }

    /**
     * 调整队列容量, 最小16, 最大65536, 其他输入参数自动调整为最接近的2次幂
     *
     * @param size buffer size
     * @return int
     */
    private int adjustSize(int size) {
        if (size < 16)
            return 16;
        if (size > 65536)
            return 65536;
        else
            return minPow2(size);
    }

    @Override
    protected void start0() {
        disruptor.start();
        log.info("Disruptor::start() func execution succeed, {} is start", name);
    }

    @Override
    protected void stop0() {
        disruptor.shutdown();
        log.info("Disruptor::shutdown() func execution succeed, {} is shutdown", name);
    }

    /**
     * @param eventType EventFactory<E>
     * @param <E>       Class type
     * @return Wizard<E>
     */
    public static <E> Wizard<E> mpHub(Class<E> eventType) {
        return mpHub(ReflectionEventFactory.newFactory(eventType, log));
    }

    /**
     * @param factory EventFactory<E>
     * @param <E>     Class type
     * @return Wizard<E>
     */
    public static <E> Wizard<E> mpHub(EventFactory<E> factory) {
        return new Wizard<>(MULTI, factory);
    }

    /**
     * @param eventType EventFactory<E>
     * @param <E>       Class type
     * @return Wizard<E>
     */
    public static <E> Wizard<E> spHub(Class<E> eventType) {
        return spHub(ReflectionEventFactory.newFactory(eventType, log));
    }

    /**
     * @param factory EventFactory<E>
     * @param <E>     Class type
     * @return Wizard<E>
     */
    public static <E> Wizard<E> spHub(EventFactory<E> factory) {
        return new Wizard<>(SINGLE, factory);
    }

    /**
     * @param translator EventTranslatorOneArg<E, A>
     * @param <A>        another object type
     * @return the new EventPublisher<E, A> object
     */
    public <A> EventPublisherArg1<E, A> newPublisher(EventTranslatorOneArg<E, A> translator)
            throws IllegalStateException {
        assertIsMultiProducer();
        return new EventPublisherArg1<>(disruptor.getRingBuffer(), translator);
    }

    /**
     * @param translator EventTranslatorTwoArg<E, A0, A1>
     * @param <A0>       another 0 object type
     * @param <A1>       another 1 object type
     * @return EventPublisherArg2<E, A0, A1>
     * @throws IllegalStateException ise
     */
    public <A0, A1> EventPublisherArg2<E, A0, A1> newPublisher(EventTranslatorTwoArg<E, A0, A1> translator)
            throws IllegalStateException {
        assertIsMultiProducer();
        return new EventPublisherArg2<>(disruptor.getRingBuffer(), translator);
    }

    /**
     * @param translator EventTranslatorThreeArg<E, A0, A1, A2>
     * @param <A0>       another 0 object type
     * @param <A1>       another 1 object type
     * @param <A2>       another 2 object type
     * @return EventPublisherArg3<E, A0, A1, A2>
     * @throws IllegalStateException ise
     */
    public <A0, A1, A2> EventPublisherArg3<E, A0, A1, A2> newPublisher(EventTranslatorThreeArg<E, A0, A1, A2> translator)
            throws IllegalStateException {
        assertIsMultiProducer();
        return new EventPublisherArg3<>(disruptor.getRingBuffer(), translator);
    }

    private void assertIsMultiProducer() {
        if (!isMultiProducer) {
            log.error("RingEventbus -> {} is not multi producer mode", name);
            throw new IllegalStateException(name + " not a Multi-Producer");
        }
    }

    /**
     * @param translator EventTranslator<E>
     */
    public void publish(EventTranslator<E> translator) {
        ringBuffer.publishEvent(translator);
    }

    /**
     * @param translator EventTranslatorOneArg<E, A>
     * @param arg        A
     * @param <A>        Arg type
     */
    public <A> void publish(EventTranslatorOneArg<E, A> translator, A arg) {
        ringBuffer.publishEvent(translator, arg);
    }

    /**
     * @param translator EventTranslatorTwoArg<E, A0, A1>
     * @param arg0       A0
     * @param arg1       A1
     * @param <A0>       A0 Type
     * @param <A1>       A1 Type
     */
    public <A0, A1> void publish(EventTranslatorTwoArg<E, A0, A1> translator, A0 arg0, A1 arg1) {
        ringBuffer.publishEvent(translator, arg0, arg1);
    }


    /**
     * @param translator EventTranslatorThreeArg<E, A0, A1, A2>
     * @param arg0       A0
     * @param arg1       A1
     * @param arg2       A2
     * @param <A0>       A0 Type
     * @param <A1>       A1 Type
     * @param <A2>       A2 Type
     */
    public <A0, A1, A2> void publish(EventTranslatorThreeArg<E, A0, A1, A2> translator, A0 arg0, A1 arg1, A2 arg2) {
        ringBuffer.publishEvent(translator, arg0, arg1, arg2);
    }

    public static class Wizard<E> {

        protected final EventFactory<E> factory;
        protected final ProducerType type;

        protected String name;
        protected int size = 256;
        protected StartMode startMode = StartMode.auto();
        protected WaitStrategy strategy = YIELDING.getInstance();

        private Wizard(ProducerType type, EventFactory<E> factory) {
            this.type = type;
            this.factory = factory;
        }

        public Wizard<E> name(String name) {
            this.name = name;
            return this;
        }

        public Wizard<E> size(int size) {
            this.size = size;
            return this;
        }

        public Wizard<E> waitStrategy(WaitStrategy strategy) {
            this.strategy = strategy;
            return this;
        }

        public Wizard<E> startMode(StartMode startMode) {
            this.startMode = startMode;
            return this;
        }

        public RingHub<E> build(EventHandler<E> handler) {
            nonNull(handler, "handler");
            return new RingHub<>(name, size, startMode, type, factory, strategy, HandlerGraph.firstWith(handler).build());
        }

        public RingHub<E> build(HandlerGraph<E> graph) {
            nonNull(graph, "graph");
            return new RingHub<>(name, size, startMode, type, factory, strategy, graph);
        }

    }


}
