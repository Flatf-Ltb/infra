package io.flatf.infra.persistence.rocksdb.map.kv;

public interface RocksReversibleKey extends RocksKey {

    /**
     * @return RocksDB reversed key byte[]
     */
    byte[] reversedKey();

}
