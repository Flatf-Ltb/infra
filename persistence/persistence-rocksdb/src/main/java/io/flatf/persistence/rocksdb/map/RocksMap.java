package io.flatf.persistence.rocksdb.map;

import io.flatf.common.thread.ShutdownHooks;
import io.flatf.persistence.rocksdb.exception.RocksRuntimeException;
import io.flatf.persistence.rocksdb.map.kv.RocksKey;
import io.flatf.persistence.rocksdb.map.kv.RocksValue;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.tuple.Pair;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Holder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

import java.io.Closeable;
import java.util.Collection;

public class RocksMap<K extends RocksKey, V extends RocksValue> implements Closeable {

    private final Options options;
    private final RocksDB rocksdb;

    public RocksMap(String savePath) throws RocksRuntimeException {
        DBOptions dbOptions = new DBOptions();
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        this.options = new Options(dbOptions, columnFamilyOptions);
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.ALL);
        options.setStatistics(statistics);
        options.setCreateIfMissing(true);
        try {
            this.rocksdb = RocksDB.open(options, savePath);
        } catch (RocksDBException e) {
            throw new RocksRuntimeException(e);
        }
        ShutdownHooks.closeResourcesWhenShutdown(this);
    }

    public static void main(String[] args) {
        try (Options options = new Options()) {
            ShutdownHooks.addShutdownHook("RocksContainerCloseThread",
                    options::close);
        }

    }

    public void scan() {

    }

    public boolean notExist(K key) {
        return !rocksdb.keyMayExist(key.key(), new Holder<>());
    }

    /**
     * @param key K
     * @return V
     */
    public V get(K key) {
        return null;
    }

    /**
     * @param key0 K
     * @param key1 K
     * @return Collection<V>
     */
    public Collection<V> scan(K key0, K key1) {
        return null;
    }

    /**
     * @param key   K
     * @param value V
     * @return V
     */
    public V put(K key, V value) {
        return null;
    }

    /**
     * @param key K
     * @return V
     */
    public V remove(K key) {
        return null;
    }

    /**
     * @param keyValues MutableSet<Pair<K, V>> keyValues
     */
    public void put(MutableSet<Pair<K, V>> keyValues) {

    }

    /**
     *
     */
    public void clear() {

    }

    @Override
    public void close() {
        if (options != null)
            options.close();

        if (rocksdb != null)
            rocksdb.close();
    }

}
