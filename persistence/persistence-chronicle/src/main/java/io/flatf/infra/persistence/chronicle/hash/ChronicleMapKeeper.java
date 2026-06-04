package io.flatf.infra.persistence.chronicle.hash;

import io.flatf.common.collections.keeper.FilesKeeper;
import io.flatf.common.lang.Validator;
import io.flatf.infra.persistence.chronicle.exception.ChronicleIOException;
import net.openhft.chronicle.map.ChronicleMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Set;

@ThreadSafe
public class ChronicleMapKeeper<K, V> extends FilesKeeper<String, ChronicleMap<K, V>> {

    private final ChronicleMapConfigurator<K, V> cfg;

    public ChronicleMapKeeper(@Nonnull ChronicleMapConfigurator<K, V> cfg) {
        Validator.nonNull(cfg, "cfg");
        this.cfg = cfg;
    }

    public ChronicleMapConfigurator<K, V> getConfigurator() {
        return cfg;
    }

    protected final Object lock = new Object();

    // 关闭状态
    protected volatile boolean isClosed = false;

    @Nonnull
    @Override
    public ChronicleMap<K, V> acquire(@Nonnull String filename) throws ChronicleIOException {
        Validator.nonEmpty(filename, "filename");
        synchronized (lock) {
            if (isClosed) {
                throw new IllegalStateException(
                        "ChronicleMapKeeper configurator of -> {" + cfg.getConfigInfo() + "} is closed");
            }
            return super.acquire(filename);
        }
    }

    @Override
    protected ChronicleMap<K, V> createWithKey(String filename) throws ChronicleIOException {
        //构建器
        return ChronicleHashStorage
                // 设置KeyClass, ValueClass, SavePath, filename
                .newMap(cfg.getKeyClass(), cfg.getValueClass(), cfg.getSavePath().getAbsolutePath(), filename)
                // 持久化选项
                .setPersistent(cfg.isPersistent())
                // 恢复选项
                .setRecover(cfg.isRecover())
                // 设置put函数是否返回null
                .setPutReturnsNull(cfg.isPutReturnsNull())
                // 设置remove函数是否返回null
                .setRemoveReturnsNull(cfg.isRemoveReturnsNull())
                // 设置条目总数
                .setEntries(cfg.getEntries())
                // 设置条目数校验
                .setChecksumEntries(false)
                // 设置块大小
                .setActualChunkSize(cfg.getActualChunkSize())
                // 设置关闭操作
                .setPreShutdownAction(null)
                //基于Key值设置平均长度
                .setAverageKey(cfg.getAverageKey())
                // 基于Value值设置平均长度
                .setAverageValue(cfg.getAverageValue()).build();
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            Set<String> keySet = savedMap.keySet();
            for (String key : keySet) {
                ChronicleMap<K, V> map = savedMap.get(key);
                if (map.isOpen()) {
                    map.close();
                }
                savedMap.remove(key);
            }
            this.isClosed = true;
        }
    }

}
