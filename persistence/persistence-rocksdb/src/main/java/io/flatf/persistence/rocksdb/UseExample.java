package io.flatf.persistence.rocksdb;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class UseExample {

    private static final String dbPath = "/tmp/rocksdb/data/";

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksdb;

    // RocksDB.DEFAULT_COLUMN_FAMILY
    public void testDefaultColumnFamily() throws RocksDBException, IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        // 文件不存在，则先创建文件
        Path path = Paths.get(dbPath);
        if (!Files.isSymbolicLink(path))
            Files.createDirectories(path);
        rocksdb = RocksDB.open(options, dbPath);

        //简单key-value
        byte[] key = "Hello".getBytes();
        byte[] value = "World".getBytes();
        rocksdb.put(key, value);

        byte[] getValue = rocksdb.get(key);
        System.out.println(new String(getValue));

        //通过List做主键查询
        rocksdb.put("SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key);
        keys.add("SecondKey".getBytes());

        List<byte[]> values = rocksdb.multiGetAsList(keys);
        for (int i = 0; i < keys.size(); i++) {
            System.out.println(new String(keys.get(i)) + ":" + new String(values.get(i)));
        }

        //打印全部[key - value]
        RocksIterator iter = rocksdb.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

        //删除一个key
        rocksdb.delete(key);
        System.out.println("after remove key:" + new String(key));

        iter = rocksdb.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }
    }

    public void testCertainColumnFamily() throws RocksDBException {
        String table = "CertainColumnFamilyTest";
        String key = "certainKey";
        String value = "certainValue";

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        Options options = new Options();
        options.setCreateIfMissing(true);

        List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);
        if (!cfs.isEmpty()) {
            for (byte[] cf : cfs) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
            }
        } else {
            columnFamilyDescriptors
                    .add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);

        rocksdb = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);
        for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
            if (new String(columnFamilyDescriptors.get(i).getName()).equals(table)) {
                rocksdb.dropColumnFamily(columnFamilyHandles.get(i));
            }
        }

        ColumnFamilyHandle columnFamilyHandle = rocksdb
                .createColumnFamily(new ColumnFamilyDescriptor(table.getBytes(), new ColumnFamilyOptions()));
        rocksdb.put(columnFamilyHandle, key.getBytes(), value.getBytes());

        byte[] getValue = rocksdb.get(columnFamilyHandle, key.getBytes());
        System.out.println("get Value : " + new String(getValue));

        rocksdb.put(columnFamilyHandle, "SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key.getBytes());
        keys.add("SecondKey".getBytes());

        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        handleList.add(columnFamilyHandle);
        handleList.add(columnFamilyHandle);

        List<byte[]> values = rocksdb.multiGetAsList(handleList, keys);
        for (int i = 0; i < keys.size(); i++) {
            System.out.println(new String(keys.get(i)) + "--" + new String(values.get(i)));
        }

        rocksdb.delete(columnFamilyHandle, key.getBytes());

        RocksIterator iter = rocksdb.newIterator(columnFamilyHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }
    }

    public static void main(String[] args) throws Exception {
        UseExample test = new UseExample();
        test.testDefaultColumnFamily();
        test.testCertainColumnFamily();
    }

}
