package io.flatf.persistence.chronicle.queue;

import io.flatf.common.annotation.AbstractFunction;
import io.flatf.common.collections.MutableMaps;
import io.flatf.common.constant.TimeZoneConst;
import io.flatf.common.file.PermissionDeniedException;
import io.flatf.common.lang.Validator;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.thread.RuntimeInterruptedException;
import io.flatf.common.thread.ShutdownHooks;
import io.flatf.common.thread.Sleep;
import io.flatf.common.thread.Threads;
import io.flatf.common.util.StringSupport;
import io.flatf.persistence.chronicle.queue.params.ReaderParams;
import lombok.Getter;
import lombok.experimental.Accessors;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.lang.Thread.State;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;

import static io.flatf.common.datetime.DateTimeUtil.datetimeOfSecond;
import static io.flatf.common.file.FileUtil.mkdir;
import static io.flatf.common.sys.SysProperties.JAVA_IO_TMPDIR;
import static io.flatf.common.sys.SysProperties.USER_HOME;

@Immutable
@Accessors(fluent = true)
public abstract class AbstractChronicleQueue<
        T, // 写入和读取类型
        AT extends AbstractChronicleAppender<T>, // 添加器类型
        RT extends AbstractChronicleReader<T>> // 读取器类型
        // 实现特定关闭对象
        implements net.openhft.chronicle.core.io.Closeable {

    @Getter
    protected final String rootPath;
    @Getter
    protected final String folder;

    protected final boolean readOnly;
    protected final long epoch;
    protected final LocalTime rollTime;
    protected final ZoneId rollTimeZone;
    protected final FileCycle fileCycle;

    // 文件清理周期
    private final int fileClearCycle;
    // 存储文件释放回调
    private final ObjIntConsumer<File> storeFileListener;

    @Getter
    protected final File savePath;
    @Getter
    protected final String queueName;
    @Getter
    protected final RollingChronicleQueue internalQueue;

    protected Logger logger = Log4j2LoggerFactory.getLogger(getClass());

    protected AbstractChronicleQueue(BaseQueueBuilder<?> builder) {
        this.rootPath = builder.rootPath;
        this.folder = builder.folder;
        this.readOnly = builder.readOnly;
        this.epoch = builder.epoch;
        this.rollTime = builder.rollTime;
        this.rollTimeZone = builder.rollTimeZone;
        this.fileCycle = builder.fileCycle;
        this.fileClearCycle = builder.fileClearCycle <= 0
                ? 0 : Math.max(10, builder.fileClearCycle);
        this.storeFileListener = builder.storeFileListener;
        if (builder.logger != null) {
            this.logger = builder.logger;
        }
        this.savePath = new File(rootPath + "chronicle-queue/" + folder);
        this.queueName = folder.replaceAll("/", "");
        this.internalQueue = buildChronicleQueue();
        createFileClearThread();
        logger.info("{} initialized -> name==[{}], desc==[{}]", getClass().getSimpleName(), queueName,
                fileCycle.getDesc());
    }

    private RollingChronicleQueue buildChronicleQueue() {
        if (!savePath.exists()) {
            // 创建存储路径
            boolean succeeded = mkdir(savePath);
            if (!succeeded)
                throw new PermissionDeniedException(savePath);
        }
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single(savePath)
                // 文件滚动周期
                .rollCycle(fileCycle.getRollCycle())
                // 是否只读
                .readOnly(readOnly)
                // 设置滚动时间
                .rollTime(rollTime)
                // 设置滚动时区
                .rollTimeZone(rollTimeZone)
                // 文件存储回调
                .storeFileListener(this::storeFileHandle);
        if (epoch > 0L) {
            builder.epoch(epoch);
        }
        // TODO 待解决CPU缓存行填充问题
        ShutdownHooks.addShutdownHook("ChronicleQueue-Cleanup", this::shutdownHandle);
        return builder.build();
    }

    /**
     * 关闭
     */
    private void shutdownHandle() {
        logger.info("ChronicleQueue [{}] shutdown hook started", queueName);
        try {
            close();
        } catch (Exception e) {
            logger.error("ChronicleQueue [{}] shutdown hook throw exception: {}", queueName, e.getMessage(), e);
        }
        logger.info("ChronicleQueue [{}] shutdown hook finished", queueName);
    }

    /**
     * ******************************* 文件清理 START ********************************
     */
    // 最后文件周期
    private AtomicInteger lastCycle;
    // 周期文件存储Map
    private ConcurrentMap<Integer, String> cycleFileMap;
    // 周期文件清理线程
    private Thread fileClearThread;
    // 周期文件清理线程运行状态
    private final AtomicBoolean isClearRunning = new AtomicBoolean(true);

    /**
     * 创建文件清理线程
     */
    private void createFileClearThread() {
        if (fileClearCycle > 0) {
            this.lastCycle = new AtomicInteger();
            this.cycleFileMap = MutableMaps.newConcurrentMap();
            // 周期文件清理间隔
            long delay = (long) fileCycle.getSeconds() * fileClearCycle;
            // 创建文件清理线程
            this.fileClearThread = Threads.startNewThread(queueName + "-FileClearThread", () -> {
                do {
                    try {
                        TimeUnit.SECONDS.sleep(delay);
                    } catch (RuntimeInterruptedException | InterruptedException e) {
                        logger.info("Last execution fileClearTask");
                        fileClearTask();
                        logger.info("Thread -> {} quit now", Threads.getCurrentThreadName());
                    }
                    if (isClearRunning.get()) {
                        fileClearTask();
                    }
                } while (isClearRunning.get());
            });
            // singleThreadScheduleWithFixedDelay(delay, delay, TimeUnit.SECONDS,
            // this::runFileClearTask);
            logger.info("Build clear thread is finished");
        }
    }

    /**
     * 文件清理任务
     */
    private void fileClearTask() {
        int last = lastCycle.get();
        // 计算需要删除的基准线
        int deleteBaseline = last - fileClearCycle;
        logger.info("Execute clear schedule : lastCycle==[{}], deleteBaseline==[{}]", last, deleteBaseline);
        // 获取全部存储文件的Key
        Set<Integer> cycleFileKeys = cycleFileMap.keySet();
        for (int saveCycle : cycleFileKeys) {
            // 小于基准线的文件被删除
            if (saveCycle < deleteBaseline) {
                String fileAbsolutePath = cycleFileMap.get(saveCycle);
                logger.info("Delete cycle file : cycle==[{}], fileAbsolutePath==[{}]", saveCycle, fileAbsolutePath);
                File file = new File(fileAbsolutePath);
                if (file.exists()) {
                    // 删除文件
                    if (file.delete()) {
                        cycleFileMap.remove(saveCycle);
                        logger.info("File : [{}] successfully deleted", file.getAbsolutePath());
                    } else {
                        logger.warn("File : [{}] delete failure", fileAbsolutePath);
                    }
                } else {
                    logger.error("File not exists, Please check the ChronicleQueue save path : [{}]",
                            savePath.getAbsolutePath());
                }
            }
        }
    }

    /**
     * @param cycle int
     * @param file  File
     */
    private void storeFileHandle(int cycle, File file) {
        logger.info("Released file : cycle==[{}], file==[{}]", cycle, file.getAbsolutePath());
        if (storeFileListener != null) {
            // 调用存储文件释放回调
            storeFileListener.accept(file, cycle);
        }
        if (fileClearCycle > 0) {
            // 如果设置了文件清理周期, 记录文件路径和最后一个文件的周期
            cycleFileMap.put(cycle, file.getAbsolutePath());
            lastCycle.set(cycle);
        }
    }




    public FileCycle fileCycle() {
        return fileCycle;
    }

    public boolean isClosed() {
        return internalQueue.isClosed();
    }

    @Override
    public void close() {
        if (isClosed())
            return;
        // 关闭外部访问器
        closeAllAccessor();
        // 关闭队列
        internalQueue.close();
        // 停止运行文件清理线程
        isClearRunning.set(false);
        // 中断正在休眠的文件清理线程
        if (fileClearThread != null) {
            fileClearThread.interrupt();
            while (fileClearThread.getState() != State.TERMINATED)
                Sleep.millis(5);
        }
    }

    /**
     * Reader counter
     */
    private final AtomicInteger readerCounter = new AtomicInteger(0);

    public int getReaderCount() {
        return readerCounter.get();
    }

    private String generateReaderName() {
        return queueName + "-reader-" + readerCounter.getAndIncrement();
    }

    private static final String EMPTY_CONSUMER_MSG = "Reader consumer is an empty implementation";

    /**
     * @return RT
     * @throws IllegalStateException ise
     */
    public RT createReader() throws IllegalStateException {
        return createReader(generateReaderName(), ReaderParams.defaultParams(), out -> logger.info(EMPTY_CONSUMER_MSG));
    }

    /**
     * @param dataConsumer Consumer<OUT>
     * @return RT
     * @throws IllegalStateException ise
     */
    public RT createReader(@Nonnull Consumer<T> dataConsumer)
            throws IllegalStateException {
        return createReader(generateReaderName(), ReaderParams.defaultParams(), dataConsumer);
    }

    /**
     * @param readerName   String
     * @param dataConsumer Consumer<OUT>
     * @return RT
     * @throws IllegalStateException ise
     */
    public RT createReader(@Nonnull String readerName,
                           @Nonnull Consumer<T> dataConsumer)
            throws IllegalStateException {
        return createReader(readerName, ReaderParams.defaultParams(), dataConsumer);
    }

    /**
     * @param param        ReaderParams
     * @param dataConsumer Consumer<OUT>
     * @return RT
     * @throws IllegalStateException ise
     */
    public RT createReader(@Nonnull ReaderParams param, @Nonnull Consumer<T> dataConsumer)
            throws IllegalStateException {
        return createReader(generateReaderName(), param, dataConsumer);
    }

    /**
     * @param readerName   String
     * @param param        ReaderParams
     * @param dataConsumer Consumer<OUT>
     * @return RT
     * @throws IllegalStateException ise
     */
    public RT createReader(@Nonnull String readerName, @Nonnull ReaderParams param, @Nonnull Consumer<T> dataConsumer)
            throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("Cannot be create reader, Chronicle queue is closed");
        }
        Validator.nonNull(readerName, "readerName");
        Validator.nonNull(param, "param");
        Validator.nonNull(dataConsumer, "dataConsumer");
        RT reader = createReader(readerName, param, logger, dataConsumer);
        addAccessor(reader);
        return reader;
    }

    /**
     * @param readerName   String
     * @param param        ReaderParams
     * @param logger       Logger
     * @param dataConsumer Consumer<OUT>
     * @return RT
     * @throws IllegalStateException ise
     */
    @AbstractFunction
    protected abstract RT createReader(@Nonnull String readerName,
                                       @Nonnull ReaderParams param,
                                       @Nonnull Logger logger,
                                       @Nonnull Consumer<T> dataConsumer) throws IllegalStateException;

    /**
     * Appender counter
     */
    private final AtomicInteger appenderCounter = new AtomicInteger(0);

    public int getAppenderCount() {
        return appenderCounter.get();
    }

    /**
     * @return String
     */
    private String generateAppenderName() {
        return queueName + "-appender-" + appenderCounter.getAndIncrement();
    }

    /**
     * @return AT
     * @throws IllegalStateException ise
     */
    public AT acquireAppender() throws IllegalStateException {
        return acquireAppender(generateAppenderName(), null);
    }

    /**
     * @param appenderName String
     * @return AT
     * @throws IllegalStateException ise
     */
    public AT acquireAppender(@Nonnull String appenderName) throws IllegalStateException {
        Validator.nonNull(appenderName, "appenderName");
        return acquireAppender(appenderName, null);
    }

    /**
     * @param dataProducer Supplier<IN>
     * @return AT
     * @throws IllegalStateException ise
     */
    public AT acquireAppender(@Nonnull Supplier<T> dataProducer) throws IllegalStateException {
        Validator.nonNull(dataProducer, "dataProducer");
        return acquireAppender(generateAppenderName(), dataProducer);
    }

    /**
     * @param appenderName String
     * @param dataProducer Supplier<IN>
     * @return AT
     * @throws IllegalStateException ise
     */
    public AT acquireAppender(@Nonnull String appenderName,
                              @CheckForNull Supplier<T> dataProducer)
            throws IllegalStateException {
        if (isClosed())
            throw new IllegalStateException("Cannot be acquire appender, Chronicle queue is closed");
        Validator.nonNull(appenderName, "appenderName");
        AT appender = acquireAppender(appenderName, logger, dataProducer);
        addAccessor(appender);
        return appender;
    }

    /**
     * @param appenderName String
     * @param logger       Logger
     * @param dataProducer Supplier<IN>
     * @return AT
     * @throws IllegalStateException ise
     */
    @AbstractFunction
    protected abstract AT acquireAppender(@Nonnull String appenderName,
                                          @Nonnull Logger logger,
                                          @CheckForNull Supplier<T> dataProducer)
            throws IllegalStateException;

    /**
     * 已分配的访问器
     */
    private final ConcurrentMap<Long, CloseableChronicleAccessor> allocatedAccessor = MutableMaps.newConcurrentMap();

    /**
     * 添加访问器
     *
     * @param accessor CloseableChronicleAccessor
     */
    private void addAccessor(CloseableChronicleAccessor accessor) {
        allocatedAccessor.put(accessor.getAllocateSeq(), accessor);
    }

    /**
     * 关闭全部访问器
     */
    private void closeAllAccessor() {
        for (CloseableChronicleAccessor accessor : allocatedAccessor.values()) {
            if (!accessor.isClosed())
                accessor.close();
        }
    }

    /**
     * Queue 构建器
     *
     * @param <B>
     * @author yellow013
     */
    protected abstract static class BaseQueueBuilder<B extends BaseQueueBuilder<B>> {

        private String rootPath = JAVA_IO_TMPDIR + File.separator;
        private String folder = "auto-created-" + datetimeOfSecond() + File.separator;
        private boolean readOnly = false;
        private long epoch = 0L;
        private LocalTime rollTime = LocalTime.MIN;
        private ZoneId rollTimeZone = TimeZoneConst.UTC;
        private FileCycle fileCycle = FileCycle.FAST_DAILY;
        private ObjIntConsumer<File> storeFileListener;
        private int fileClearCycle = 0;

        private Logger logger;

        public B saveToUserHome() {
            return rootPath(USER_HOME);
        }

        public B saveToTmpDir() {
            return rootPath(JAVA_IO_TMPDIR);
        }

        public B rootPath(@Nonnull String rootPath) {
            this.rootPath = StringSupport.fixPath(rootPath);
            return self();
        }

        public B folder(@Nonnull String folder) {
            this.folder = StringSupport.fixPath(folder);
            return self();
        }

        public B topic(@Nonnull String topic) {
            return topic(topic, "");
        }

        public B topic(@Nonnull String topic, @Nullable String... subtopics) {
            this.folder = StringSupport.fixPath(topic);
            return self();
        }

        public B enableReadOnly() {
            this.readOnly = true;
            return self();
        }

        public B epoch(long epoch) {
            this.epoch = epoch;
            return self();
        }

        public B rollTime(@Nonnull LocalTime rollTime) {
            Validator.nonNull(rollTime, "rollTime");
            this.rollTime = rollTime;
            return self();
        }

        public B rollTimeZone(@Nonnull ZoneId rollTimeZone) {
            Validator.nonNull(rollTimeZone, "rollTimeZone");
            this.rollTimeZone = rollTimeZone;
            return self();
        }

        public B fileCycle(@Nonnull FileCycle fileCycle) {
            Validator.nonNull(fileCycle, "fileCycle");
            this.fileCycle = fileCycle;
            return self();
        }

        public B fileClearCycle(int fileClearCycle) {
            this.fileClearCycle = fileClearCycle;
            return self();
        }

        public B storeFileListener(@Nonnull ObjIntConsumer<File> storeFileListener) {
            Validator.nonNull(storeFileListener, "storeFileListener");
            this.storeFileListener = storeFileListener;
            return self();
        }

        public B logger(@Nonnull Logger logger) {
            Validator.nonNull(logger, "logger");
            this.logger = logger;
            return self();
        }

        @AbstractFunction
        protected abstract B self();

    }

}
