package io.flatf.transport.rmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.flatf.common.collections.MutableLists;
import io.flatf.common.datetime.DateTimeUtil;
import io.flatf.common.lang.Validator;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.serialization.specific.BytesDeserializer;
import io.flatf.transport.api.Subscriber;
import io.flatf.transport.rmq.config.RmqConsumerCfg;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.collections.api.list.MutableList;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static io.flatf.common.util.StringSupport.nonEmpty;

/**
 * @author yellow013
 * <p>
 * 处理批量数据, 手动ACK
 */
public class RmqBatchReceiver<T> extends RmqTransport implements Subscriber, Runnable {

    private static final Logger log = Log4j2LoggerFactory.getLogger(RmqBatchReceiver.class);

    // 接收者名称
    private String receiverName;

    // 队列名称
    private final String receiveQueue;

    // 队列持久化
    private boolean durable = true;

    public RmqBatchReceiver<T> setDurable(boolean durable) {
        this.durable = durable;
        return this;
    }

    // 连接独占此队列
    private boolean exclusive = false;

    public RmqBatchReceiver<T> setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
        return this;
    }

    // channel关闭后自动删除队列
    private boolean autoDelete = false;

    public RmqBatchReceiver<T> setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
        return this;
    }

    private final BatchProcessConsumer<T> consumer;

    public RmqBatchReceiver(String tag, @Nonnull RmqConsumerCfg cfg, long autoFlushInterval,
                            BytesDeserializer<T> deserializer, BatchHandler<T> batchHandler, RefreshNowEvent<T> refreshNowEvent) {
        super(nonEmpty(tag) ? tag : "batch-recv-" + DateTimeUtil.datetimeOfMillisecond(), cfg.getConnection());
        this.receiveQueue = cfg.queue().getQueueName();
        createConnection();
        queueDeclare();
        this.consumer = new BatchProcessConsumer<>(channel, cfg.ackOptions().qos(), autoFlushInterval,
            batchHandler, deserializer, refreshNowEvent, null);
    }

    public RmqBatchReceiver(String tag, @Nonnull RmqConsumerCfg configurator, long autoFlushInterval,
                            BytesDeserializer<T> deserializer, BatchHandler<T> batchHandler, RefreshNowEvent<T> refreshNowEvent,
                            Predicate<T> filter) {
        super(nonEmpty(tag) ? tag : "batch-receiver-" + DateTimeUtil.datetimeOfMillisecond(),
            configurator.getConnection());
        this.receiveQueue = configurator.queue().getQueueName();
        createConnection();
        queueDeclare();
        this.consumer = new BatchProcessConsumer<>(super.channel, configurator.ackOptions().qos(),
            autoFlushInterval, batchHandler, deserializer, refreshNowEvent, filter);
    }

    private void queueDeclare() {
        this.receiverName = "receiver::" + rmqConnection.getConfigInfo() + "$" + receiveQueue;
        try {
            channel.queueDeclare(receiveQueue, durable, exclusive, autoDelete, null);
        } catch (IOException e) {
            log.error(
                "Function channel.queueDeclare(queue==[{}], durable==[{}], exclusive==[{}], autoDelete==[{}], arguments==null) IOException message -> {}",
                receiveQueue, durable, exclusive, autoDelete, e.getMessage(), e);
            closeIgnoreException();
        }
    }

    @Override
    public void run() {
        subscribe();
    }

    @Override
    public void subscribe() {
        try {
            channel.basicConsume(receiveQueue, false, tag, consumer);
        } catch (IOException e) {
            log.error("basicConsume error : {}", e.getMessage(), e);
        }
    }

    @Override
    public String getName() {
        return receiverName;
    }

    @Override
    public boolean closeIgnoreException() {
        log.info("Call method RabbitMqReceiver.destroy()");
        closeConnection();
        return true;
    }

    @Override
    public void reconnect() {
        // TODO Auto-generated method stub
    }

    /**
     * @author yellow013
     * <p>
     *         TODO 改进为实现function函数
     */
    public static class BatchProcessConsumer<T> extends DefaultConsumer {

        private static final Logger log = Log4j2LoggerFactory.getLogger(BatchProcessConsumer.class);

        private Channel channel;

        private final BatchHandler<T> batchHandler;

        private final BytesDeserializer<T> deserializer;

        /**
         * prefetchCount : rabbit consumer最多接受的数量
         */
        private int prefetchCount;
        /**
         * seconds: automatic flush check in the specified time
         */
        private long millisecond = 500L;
        /**
         * 上一次的buffer -> (bufferList) 大小
         */
        private volatile long lastSize;
        /**
         * 缓存计数器
         */
        private LongAdder cacheSize;
        /**
         * last rabbitmq message sequence
         */
        private volatile long lastDeliveryTag;

        private ReentrantLock lock;

        private MutableList<T> bufferList;

        private Predicate<T> filter;

        private RefreshNowEvent<T> refreshNowEvent;

        private ScheduledExecutorService schedule;

        /**
         * constructor
         *
         * @param channel       Rabbit channel
         * @param prefetchCount 从队列中获取prefetchCount数量的消息
         * @param millisecond   自动flush的时间间隔
         * @param batchHandler  当达到prefetchCount值或自动flush触发此回调
         */
        public BatchProcessConsumer(Channel channel, int prefetchCount,
                                    long millisecond, BatchHandler<T> batchHandler,
                                    BytesDeserializer<T> deserializer,
                                    RefreshNowEvent<T> refreshNowEvent,
                                    Predicate<T> filter) {
            super(channel);
            this.channel = channel;
            this.refreshNowEvent = refreshNowEvent;
            this.filter = filter;
            this.batchHandler = Validator.nonNull(batchHandler, "batchHandler");
            this.deserializer = Validator.nonNull(deserializer, "deserializer");
            this.prefetchCount = prefetchCount;
            if (millisecond > 0) {
                this.millisecond = millisecond;
            } else {
                log.warn("Use default millisecond: {}", this.millisecond);
            }
            init();
        }

        public BatchProcessConsumer(Channel channel, BytesDeserializer<T> serializable, BatchHandler<T> batchHandler) {
            super(channel);
            Validator.nonNull(batchHandler, "qosBatchHandler");
            Validator.nonNull(serializable, "deserializer");
            this.channel = channel;
            this.batchHandler = batchHandler;
            this.deserializer = serializable;
            init();
        }

        private void init() {
            try {
                channel.basicQos(prefetchCount);
            } catch (IOException e) {
                log.error("Set prefetchCount==[{}] failure", prefetchCount, e);
                return;
            }
            cacheSize = new LongAdder();
            bufferList = MutableLists.newFastList();

            lock = new ReentrantLock();
            ThreadFactory namedThreadFactory = new BasicThreadFactory.Builder()
                .namingPattern("BatchHandlerAutoFlush-pool-%d").build();
            schedule = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
            automaticFlush();
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                   byte[] body) {
            // 序列化
            T t = deserializer.deserialization(body);
            // 过滤器
            if (filter != null && !filter.test(t)) {
                return;
            }
            lock.lock();
            try {
                save(t, envelope.getDeliveryTag());
            } finally {
                lock.unlock();
            }
        }

        private void save(T t, long lastDeliveryTag) {
            this.lastDeliveryTag = lastDeliveryTag;
            if (Objects.nonNull(t)) {
                cacheSize.increment();
                bufferList.add(t);
            }
            if (cacheSize.longValue() >= prefetchCount) {
                log.info("The message to be stored reaches the threshold[{}] -> {} , local deliveryTag[{}] ",
                    cacheSize.longValue(), prefetchCount, this.lastDeliveryTag);
                flush();
            } else if (refreshNowEvent != null && refreshNowEvent.flushNow(t)) {
                log.info("-----触发立刻刷新事件! tag -> {}-----", lastDeliveryTag);
                flush();
            }

        }

        /**
         * batch flush queue message
         */
        private void flush() {
            lock.lock();
            try {
                if (cacheSize.longValue() != 0) {
                    if (batchHandler.handle(bufferList)) {
                        channel.basicAck(lastDeliveryTag, true);
                        log.info("ack tag -> {}", lastDeliveryTag);
                        bufferList.clear();
                        cacheSize.reset();
                        log.info("cache clear, current size -> {}, cache -> {} , local tag -> {}", bufferList.size(),
                            cacheSize.longValue(), lastDeliveryTag);
                    } else {
                        // TODO do failure handle
                        log.warn("ack tag -> {}", lastDeliveryTag);
                    }
                }
            } catch (IOException e) {
                // TODO do reconnect and clear cache
                log.error("basicAck throw IOException -> message==[{}]", e.getMessage(), e);
            } catch (Exception e) {
                log.error("batch process failure, deliverTag[{}]", this.lastDeliveryTag, e);
            } finally {
                lock.unlock();
            }
        }

        /**
         * schedule task : 如果缓存中的数据长期没有变动, 触发flush动作 1. <br>
         * 当缓存队列中的值等于上一次定时任务触发的值, 并且该值大于0, 不等于RMQ预取值, 则认为该缓存数据长期没有变动, 立即触发回调.
         * <p>
         * 缓存值达到预取值的大小交给主线程去触发刷新.减少锁的竞争.
         */
        private void automaticFlush() {
            schedule.scheduleWithFixedDelay(() -> {
                try {
                    if (cacheSize.longValue() == lastSize && cacheSize.longValue() > 0
                        && cacheSize.longValue() != prefetchCount) {
                        log.info("automatic flush cache ...{} -> {} ,deliveryTag[{}]", cacheSize.longValue(), lastSize,
                            this.lastDeliveryTag);
                        flush();
                    } else {
                        lastSize = cacheSize.longValue();
                    }
                } catch (Exception e) {
                    log.error("automatic flush execute failure, deliveryTag==[{}]", this.lastDeliveryTag, e);
                }
            }, 0, millisecond, TimeUnit.MILLISECONDS);
        }

        @SuppressWarnings("unused")
        private void destroy() {
            if (!schedule.isShutdown()) {
                schedule.shutdown();
                schedule = null;
            }
            if (channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    log.error("close channel failure", e);
                } catch (TimeoutException e) {
                    log.error("close channel timeout ", e);
                }
            }
            channel = null;
        }
    }

    /**
     * @author yellow013
     */
    @FunctionalInterface
    public interface BatchHandler<T> extends Predicate<Collection<T>> {

        boolean handle(Collection<T> collection);

        @Override
        default boolean test(Collection<T> collection) {
            return handle(collection);
        }

    }

    /**
     * @author yellow013
     */
    @FunctionalInterface
    public interface RefreshNowEvent<T> extends Predicate<T> {

        boolean flushNow(T t);

        @Override
        default boolean test(T t) {
            return flushNow(t);
        }

    }

}
