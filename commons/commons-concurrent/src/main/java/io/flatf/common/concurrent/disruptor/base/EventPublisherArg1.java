package io.flatf.common.concurrent.disruptor.base;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

import javax.annotation.Nonnull;

/**
 * 事件发布者, 用于将输入<A>类型参数转换为<E>类型事件, 并调用RingBuffer对象的publishEvent函数
 * <p>
 * 并负责传递EventTranslator实现
 *
 * @param <E> Event type
 * @param <A> Arg
 * @author yellow013
 */
public final class EventPublisherArg1<E, A> {

    private final RingBuffer<E> ringBuffer;

    private final EventTranslatorOneArg<E, A> translator;

    public EventPublisherArg1(@Nonnull RingBuffer<E> ringBuffer,
                              @Nonnull EventTranslatorOneArg<E, A> translator) {
        this.ringBuffer = ringBuffer;
        this.translator = translator;
    }

    public void publish(A arg) {
        ringBuffer.publishEvent(translator, arg);
    }

}
