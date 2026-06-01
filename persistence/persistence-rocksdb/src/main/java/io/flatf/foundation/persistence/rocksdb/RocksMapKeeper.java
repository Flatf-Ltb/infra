package io.flatf.foundation.persistence.rocksdb;

import io.flatf.foundation.common.collections.keeper.AbstractKeeper;
import io.flatf.foundation.persistence.rocksdb.map.RocksMap;
import io.flatf.foundation.persistence.rocksdb.map.kv.RocksKey;
import io.flatf.foundation.persistence.rocksdb.map.kv.RocksValue;

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
