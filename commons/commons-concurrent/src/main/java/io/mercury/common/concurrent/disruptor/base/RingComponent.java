package io.mercury.common.concurrent.disruptor.base;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.mercury.common.thread.RunnableComponent;
import org.slf4j.Logger;

import static com.lmax.disruptor.dsl.ProducerType.MULTI;
import static io.mercury.common.datetime.pattern.impl.DateTimePattern.YYYYMMDD_L_HHMMSSSSS;
import static io.mercury.common.lang.Validator.nonNull;
import static io.mercury.common.log4j2.Log4j2LoggerFactory.getLogger;
import static io.mercury.common.thread.ThreadFactoryImpl.ofPlatform;
import static io.mercury.common.thread.ThreadPriority.MAX;
import static io.mercury.common.util.BitOperator.minPow2;
import static io.mercury.common.util.StringSupport.requireNonEmptyElse;
import static java.time.LocalDateTime.now;
import static java.util.Objects.requireNonNullElse;

/**
 * 抽象Disruptor实现
 *
 * @param <E> 事件处理类型
 * @param <I> 发布类型
 * @author yellow013
 */
public abstract class RingComponent<E, I> extends RunnableComponent {

    private static final Logger log = getLogger(RingComponent.class);

    protected final Disruptor<E> disruptor;

    protected final EventPublisherArg1<E, I> publisher;

    protected final boolean isMultiProducer;

    /**
     * @param name       String
     * @param size       int
     * @param type       ProducerType
     * @param strategy   WaitStrategy
     * @param factory    EventFactory<E>
     * @param translator EventTranslatorOneArg<E, I>
     */
    protected RingComponent(String name, int size, ProducerType type,
                            WaitStrategy strategy, EventFactory<E> factory,
                            EventTranslatorOneArg<E, I> translator) {
        super(requireNonEmptyElse(name,
                "RingBuf-[" + YYYYMMDD_L_HHMMSSSSS.fmt(now()) + "]"));
        nonNull(factory, "EventFactory");
        nonNull(translator, "EventTranslator");
        final ProducerType producerType = requireNonNullElse(type, MULTI);
        this.disruptor = new Disruptor<>(
                // 设置事件工厂, 调整并设置队列容量
                factory, adjustSize(size),
                // 使用最高优先级的线程工厂, 使用平台线程
                ofPlatform(this.name + "-worker").priority(MAX).build(),
                // 生产者策略, Waiting策略
                producerType,
                requireNonNullElse(strategy, CommonStrategy.Sleeping.get())
        );
        this.isMultiProducer = producerType == MULTI;
        this.publisher = new EventPublisherArg1<>(disruptor.getRingBuffer(), translator);
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
     * @param in input event
     * @return boolean
     */
    public boolean publishEvent(I in) {
        try {
            if (isClosed.get())
                return false;
            publisher.publish(in);
            return true;
        } catch (Exception e) {
            log.error("{} -> EventPublisher::handle(in) func throws exception -> [{}]",
                    name, e.getMessage(), e);
            return false;
        }
    }

    /**
     * @return EventPublisher<E, I>
     */
    public EventPublisherArg1<E, I> getPublisher() {
        return publisher;
    }

    /**
     * @param translator EventTranslatorOneArg<E, A>
     * @param <A>        another object type
     * @return the new EventPublisher<E, A> object
     */
    public <A> EventPublisherArg1<E, A> newPublisher(EventTranslatorOneArg<E, A> translator)
            throws IllegalStateException {
        if (isMultiProducer)
            return new EventPublisherArg1<>(disruptor.getRingBuffer(), translator);
        else {
            log.error("RingBuffer -> {} is not multi producer mode", name);
            throw new IllegalStateException("isMultiProducer == false");
        }
    }

    /**
     * 调整队列容量, 最小16, 最大65536, 其他输入参数自动调整为最接近的2次幂
     *
     * @param size buffer size
     * @return int
     */
    private int adjustSize(int size) {
        return size < 16 ? 16 : size > 65536 ? 65536 : minPow2(size);
    }

}