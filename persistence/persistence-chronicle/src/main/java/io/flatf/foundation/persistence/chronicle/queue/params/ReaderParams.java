package io.flatf.foundation.persistence.chronicle.queue.params;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.time.Duration;

@Getter
public final class ReaderParams {

    // 是否读取失败后关闭线程
    private final boolean failCrash;
    // 是否读取失败后记录日志
    private final boolean failLogging;
    // 读取时间
    private final Duration intervalTime;
    // 延迟读取时间
    private final Duration delayReadTime;
    // 是否等待数据写入
    private final boolean waitingData;
    // 是否自旋等待
    private final boolean spinWaiting;
    // 是否以异步方式退出
    private final boolean asyncExit;
    // 退出函数
    private final Runnable exitTask;

    private ReaderParams(Builder builder) {
        this.failCrash = builder.failCrash;
        this.failLogging = builder.failLogging;
        this.intervalTime = builder.intervalTime;
        this.delayReadTime = builder.delayReadTime;
        this.waitingData = builder.waitingData;
        this.spinWaiting = builder.spinWaiting;
        this.asyncExit = builder.asyncExit;
        this.exitTask = builder.exitTask;
    }

    /**
     * @return Builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * @return ReaderParams
     */
    public static ReaderParams defaultParams() {
        return new Builder().build();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    public static class Builder {

        // 读取失败崩溃
        private boolean failCrash = false;
        // 读取失败打印Log
        private boolean failLogging = true;
        // 是否等待新数据
        private boolean waitingData = true;
        // 是否自旋等待
        private boolean spinWaiting = false;

        // 读取间隔
        private Duration intervalTime = Duration.ofMillis(10); // 默认10ms读取一次
        // 开始读取延迟时间
        private Duration delayReadTime = Duration.ofMillis(0); // 默认不延迟, 设置为0ms

        // 退出方式
        private boolean asyncExit = false;
        // 退出函数
        private Runnable exitTask = null;

        /**
         * @return ReaderParams
         */
        public ReaderParams build() {
            return new ReaderParams(this);
        }

    }
}
