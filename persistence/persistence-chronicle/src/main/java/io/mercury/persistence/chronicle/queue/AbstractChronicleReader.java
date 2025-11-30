package io.mercury.persistence.chronicle.queue;

import io.mercury.common.annotation.AbstractFunction;
import io.mercury.common.annotation.thread.OnlyAllowSingleThreadAccess;
import io.mercury.common.datetime.TimeConst;
import io.mercury.persistence.chronicle.exception.ChronicleReadException;
import io.mercury.persistence.chronicle.queue.params.ReaderParams;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerState;
import org.slf4j.Logger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.mercury.common.datetime.DateTimeUtil.fmtDateTime;
import static io.mercury.common.datetime.pattern.impl.DateTimePattern.YY_MM_DD_HH_MM_SS_SSS;
import  io.mercury.common.thread.Sleep;
import static io.mercury.common.thread.Threads.startNewThread;

@Immutable
@NotThreadSafe
public abstract class AbstractChronicleReader<T> extends CloseableChronicleAccessor implements Runnable {

    protected final FileCycle fileCycle;

    protected final ReaderParams params;

    protected final ExcerptTailer tailer;

    protected final Consumer<T> dataConsumer;

    /**
     * @param allocateSeq  long
     * @param readerName   String
     * @param fileCycle    FileCycle
     * @param params       ReaderParams
     * @param logger       Logger
     * @param tailer       ExcerptTailer
     * @param dataConsumer Consumer<OUT>
     */
    protected AbstractChronicleReader(long allocateSeq,
                                      String readerName,
                                      FileCycle fileCycle,
                                      ReaderParams params,
                                      Logger logger,
                                      ExcerptTailer tailer,
                                      Consumer<T> dataConsumer) {
        super(allocateSeq, readerName, logger);
        this.fileCycle = fileCycle;
        this.params = params;
        this.tailer = tailer;
        this.dataConsumer = dataConsumer;
    }

    public ExcerptTailer getExcerptTailer() {
        return tailer;
    }

    public boolean moveTo(@Nonnull LocalDate date) {
        return moveTo(date.toEpochDay() * TimeConst.SECONDS_PER_DAY);
    }

    public boolean moveTo(@Nonnull LocalDateTime dateTime, @Nonnull ZoneId zoneId) {
        return moveTo(ZonedDateTime.of(dateTime, zoneId));
    }

    public boolean moveTo(@Nonnull ZonedDateTime dateTime) {
        return moveTo(dateTime.toEpochSecond());
    }

    /**
     * Move cursor to input epoch seconds.
     *
     * @param epochSecond long
     * @return boolean
     */
    public boolean moveTo(long epochSecond) {
        return tailer.moveToIndex(fileCycle.toIndex(epochSecond));
    }

    /**
     * 移动到开头
     */
    public void toStart() {
        tailer.toStart();
    }

    /**
     * 移动到结尾
     */
    public void toEnd() {
        tailer.toEnd();
    }

    /**
     * @return int
     */
    public int getCycle() {
        return tailer.cycle();
    }

    /**
     * @return long
     */
    public long getEpochSecond() {
        return ((long) tailer.cycle()) * fileCycle.getSeconds();
    }

    /**
     * @return long
     */
    public long getIndex() {
        return tailer.index();
    }

    /**
     * @return TailerState
     */
    public TailerState getState() {
        return tailer.state();
    }

    /**
     * @return String
     */
    public String getReaderName() {
        return accessorName;
    }

    /**
     * @return Thread
     */
    public Thread runWithNewThread() {
        return runWithNewThread(accessorName);
    }

    /**
     * @param threadName String
     * @return Thread
     */
    public Thread runWithNewThread(String threadName) {
        return startNewThread(threadName, this);
    }

    /**
     * @return OUT
     */
    @AbstractFunction
    protected abstract T next0();

    /**
     * Get next element of current cursor position.
     *
     * @return OUT
     * @throws IllegalStateException  ise
     * @throws ChronicleReadException cre
     */
    @CheckForNull
    @OnlyAllowSingleThreadAccess
    public T next() throws IllegalStateException, ChronicleReadException {
        if (isClose)
            throw new IllegalStateException("Unable to read next, Chronicle queue is closed");
        try {
            return next0();
        } catch (Exception e) {
            throw new ChronicleReadException(e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        logger.info("ChronicleReader -> [{}] is running at [{}]", getReaderName(), fmtDateTime(YY_MM_DD_HH_MM_SS_SSS));
        if (params.getDelayReadTime() > 0)
            Sleep.time(params.getDelayReadUnit(), params.getDelayReadTime());
        boolean waitingData = params.isWaitingData();
        boolean spinWaiting = params.isSpinWaiting();
        TimeUnit readIntervalUnit = params.getReadIntervalUnit();
        long readIntervalTime = params.getReadIntervalTime();
        for (; ; ) {
            if (isClose) {
                logger.info("ChronicleReader -> [{}] is closed, execute exit function at [{}]", getReaderName(),
                        fmtDateTime(YY_MM_DD_HH_MM_SS_SSS));
                exit();
                break;
            }
            T next = null;
            try {
                next = next();
            } catch (ChronicleReadException e) {
                if (params.isReadFailLogging())
                    logger.error("ChronicleReader -> [{}] call next throw exception: [{}] at [{}]", getReaderName(),
                            e.getMessage(), fmtDateTime(YY_MM_DD_HH_MM_SS_SSS), e);
                if (params.isReadFailCrash())
                    throw e;
            }
            if (next == null) {
                // 等待新数据
                if (waitingData) {
                    // 非自旋等待, 进入休眠
                    if (!spinWaiting) {
                        Sleep.time(readIntervalUnit, readIntervalTime);
                    }
                } else {
                    // 数据读取完毕, 退出线程
                    exit();
                    break;
                }
            } else {
                dataConsumer.accept(next);
            }
        }
    }

    private void exit() {
        final Runnable exitTask = params.getExitTask();
        if (exitTask != null) {
            if (params.isAsyncExit())
                // 异步执行退出函数
                startNewThread(getReaderName() + "-exit", exitTask);
            else
                // 同步执行退出函数
                exitTask.run();

        }
        logger.info("ChronicleReader -> [{}] running exit at {}", getReaderName(), fmtDateTime(YY_MM_DD_HH_MM_SS_SSS));
    }

    @Override
    protected void close0() {
        logger.info("Reader -> {} is closed.", getReaderName());
    }

}
