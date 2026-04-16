package io.flatf.persistence.rocksdb.map;

import io.flatf.common.thread.Threads;
import io.flatf.persistence.rocksdb.map.kv.RocksReversibleKey;
import io.flatf.persistence.rocksdb.map.kv.RocksValue;
import org.rocksdb.Options;

import java.util.Collection;

public class RocksReversibleMap<K extends RocksReversibleKey, V extends RocksValue> {

    public static void main(String[] args) {

        try (Options options = new Options()) {
            Runtime.getRuntime()
                    .addShutdownHook(Threads.newThread(
                            "RocksContainerCloseThread", options::close));
        }

    }

    public V get(K key) {
        return null;
    }

    public V get(byte[] key) {
        return null;
    }

    public V put(K key, V value) {
        return value;
    }

    public Collection<V> scan() {
        return null;
    }

}
