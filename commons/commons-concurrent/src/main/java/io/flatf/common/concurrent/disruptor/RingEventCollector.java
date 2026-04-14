package io.flatf.common.concurrent.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;

/**
 * Ring Event Station
 * 融合了 RingEventbus 和 EventHandler 的抽象类
 *
 * @param <E> Event Type
 */
public abstract class RingEventCollector<E> implements EventHandler<E> {

    protected final RingComponent<E> eventbus;

    protected RingEventCollector(Builder builder, EventFactory<E> factory) {
        if (builder.isSingleProducer) {
            this.eventbus = RingComponent.singleProducer(factory)
                    .size(builder.size)
                    .name(builder.name)
                    .waitStrategy(builder.waitStrategy)
                    .withHandler(this);
        } else {
            this.eventbus = RingComponent.multiProducer(factory)
                    .size(builder.size)
                    .name(builder.name)
                    .waitStrategy(builder.waitStrategy)
                    .withHandler(this);
        }
    }

    public static Builder singleProducer() {
        return new Builder(true);
    }

    public static Builder multiProducer() {
        return new Builder(false);
    }

    public static class Builder {

        private final boolean isSingleProducer;
        private String name = "eventbus";
        private int size = 64;
        private WaitStrategy waitStrategy = SimpleWaitStrategy.YIELDING.getInstance();

        private Builder(boolean isSingleProducer) {
            this.isSingleProducer = isSingleProducer;
        }

        public Builder size(int size) {
            this.size = size;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder waitStrategy(WaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
            return this;
        }

    }

}
