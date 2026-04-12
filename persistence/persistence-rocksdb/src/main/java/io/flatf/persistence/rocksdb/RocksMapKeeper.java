package io.flatf.persistence.rocksdb;

import io.flatf.common.collections.keeper.AbstractKeeper;
import io.flatf.persistence.rocksdb.map.RocksMap;
import io.flatf.persistence.rocksdb.map.kv.RocksKey;
import io.flatf.persistence.rocksdb.map.kv.RocksValue;

public class RocksMapKeeper<K extends RocksKey, V extends RocksValue>
        extends AbstractKeeper<String, RocksMap<K, V>> {

    @Override
    protected RocksMap<K, V> createWithKey(String k) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        
    }

}
