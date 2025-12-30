package io.mercury.persistence.chronicle.hash;

import io.mercury.common.collections.Capacity;
import io.mercury.common.datetime.DateTimeUtil;
import io.mercury.common.file.FileUtil;
import io.mercury.persistence.chronicle.exception.ChronicleIOException;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;

import static io.mercury.common.sys.SysProperties.JAVA_IO_TMPDIR;
import static io.mercury.common.util.StringSupport.fixPath;

public class ChronicleHashStorage {

    public static <K, V> MapBuilder<K, V> newMap(@Nonnull Class<K> keyType,
                                                 @Nonnull Class<V> valueType) {
        return newMap(keyType, valueType, "auto-create-map-" + DateTimeUtil.datetimeOfSecond());
    }

    public static <K, V> MapBuilder<K, V> newMap(@Nonnull Class<K> keyType,
                                                 @Nonnull Class<V> valueType,
                                                 @Nonnull String filename) {
        return newMap(keyType, valueType, JAVA_IO_TMPDIR + "/chronicle-map/", filename);
    }

    public static <K, V> MapBuilder<K, V> newMap(@Nonnull Class<K> keyType,
                                                 @Nonnull Class<V> valueType,
                                                 @Nonnull String savePath,
                                                 @Nonnull String filename) {
        return new MapBuilder<>(keyType, valueType, savePath, filename);
    }


    public static <E> SetBuilder<E> newSet(@Nonnull Class<E> elementType) {
        return newSet(elementType, "auto-create-set-" + DateTimeUtil.datetimeOfSecond());
    }

    public static <E> SetBuilder<E> newSet(@Nonnull Class<E> elementType,
                                           @Nonnull String filename) {
        return newSet(elementType, JAVA_IO_TMPDIR + "/chronicle-set/", filename);
    }

    public static <E> SetBuilder<E> newSet(@Nonnull Class<E> elementType,
                                           @Nonnull String savePath,
                                           @Nonnull String filename) {
        return new SetBuilder<>(elementType, savePath, filename);
    }


    private abstract static class HashBuilder<B extends HashBuilder<B>> {

        protected final File saveFile;

        protected boolean isRecover = false;
        protected boolean isPersistent = true;

        protected long entries = 4096;
        protected int actualChunkSize;

        protected boolean isChecksumEntries = false;

        protected Runnable preShutdownAction;

        protected HashBuilder(@Nonnull String savePath, @Nonnull String filename) {
            this.saveFile = new File(fixPath(savePath), filename);
        }

        protected abstract B self();

        public B setRecover(boolean isRecover) {
            this.isRecover = isRecover;
            return self();
        }

        public B setPersistent(boolean isPersistent) {
            this.isPersistent = isPersistent;
            return self();
        }

        public B setEntries(long entries) {
            this.entries = entries;
            return self();
        }

        public B setEntries(Capacity capacity) {
            this.entries = Capacity.checkAndGet(capacity);
            return self();
        }

        public B setActualChunkSize(int actualChunkSize) {
            this.actualChunkSize = actualChunkSize;
            return self();
        }

        public B setChecksumEntries(boolean isChecksumEntries) {
            this.isChecksumEntries = isChecksumEntries;
            return self();
        }

        public B setPreShutdownAction(Runnable preShutdownAction) {
            this.preShutdownAction = preShutdownAction;
            return self();
        }

        protected void checkSaveFile() {
            if (!saveFile.exists()) {
                // 创建文件目录
                FileUtil.mkdir(saveFile.getParentFile());
            }
        }
    }


    /**
     * ChronicleMap 构建器
     *
     * @param <K>
     * @param <V>
     */
    public static class MapBuilder<K, V> extends HashBuilder<MapBuilder<K, V>> {

        private final Class<K> keyType;
        private final Class<V> valueType;
        private final String name;

        private K averageKey;
        private V averageValue;

        private boolean isPutReturnsNull = false;
        private boolean isRemoveReturnsNull = false;

        public MapBuilder(@Nonnull Class<K> keyType, @Nonnull Class<V> valueType,
                          @Nonnull String savePath, @Nonnull String filename) {
            super(savePath, filename);
            this.keyType = keyType;
            this.valueType = valueType;
            this.name = "[SavedFile==" + saveFile.getAbsolutePath() + "]:" +
                    ":[KeyType==" + keyType.getSimpleName() + ",ValueType==" + valueType.getSimpleName() + "]";
        }

        public MapBuilder<K, V> setAverageKey(K averageKey) {
            this.averageKey = averageKey;
            return this;
        }

        public MapBuilder<K, V> setAverageValue(V averageValue) {
            this.averageValue = averageValue;
            return this;
        }

        public MapBuilder<K, V> setPutReturnsNull(boolean isPutReturnsNull) {
            this.isPutReturnsNull = isPutReturnsNull;
            return this;
        }

        public MapBuilder<K, V> setRemoveReturnsNull(boolean isRemoveReturnsNull) {
            this.isRemoveReturnsNull = isRemoveReturnsNull;
            return this;
        }

        @Override
        protected MapBuilder<K, V> self() {
            return this;
        }

        /**
         * @return ChronicleMap<K, V>
         */
        public ChronicleMap<K, V> build() {
            // ChronicleMap构建器
            ChronicleMapBuilder<K, V> mapBuilder = ChronicleMapBuilder
                    // 设置KeyType, ValueType
                    .of(keyType, valueType)
                    // 设置put函数是否返回null
                    .putReturnsNull(isPutReturnsNull)
                    // 设置remove函数是否返回null
                    .removeReturnsNull(isRemoveReturnsNull)
                    // 设置名称
                    .name(name)
                    // 设置条目总数
                    .entries(entries);

            // 设置块大小
            if (actualChunkSize > 0)
                mapBuilder.actualChunkSize(actualChunkSize);
            // 设置关闭操作
            if (preShutdownAction != null)
                mapBuilder.setPreShutdownAction(preShutdownAction);
            // 基于Key值设置平均长度
            if (averageKey != null)
                mapBuilder.averageKey(averageKey);
            // 基于Value值设置平均长度
            if (averageValue != null)
                mapBuilder.averageValue(averageValue);

            // 持久化选项
            if (isPersistent) {
                checkSaveFile();
                try {
                    // Is recover data
                    return isRecover ? mapBuilder.recoverPersistedTo(saveFile, true)
                            : mapBuilder.createPersistedTo(saveFile);
                } catch (IOException e) {
                    throw new ChronicleIOException(e);
                }
            } else {
                // 设置条目数校验
                mapBuilder.checksumEntries(isChecksumEntries);
                return mapBuilder.create();
            }
        }
    }

    /**
     * ChronicleSet 构建器
     *
     * @param <E>
     */
    public static class SetBuilder<E> extends HashBuilder<SetBuilder<E>> {

        private final Class<E> elementType;
        private final String name;

        private E averageElement;

        public SetBuilder(@Nonnull Class<E> elementType,
                          @Nonnull String savePath, @Nonnull String filename) {
            super(savePath, filename);
            this.elementType = elementType;
            this.name = "[SavedFile==" + saveFile.getAbsolutePath() + "]:" +
                    ":[ElementType==" + elementType.getSimpleName() + "]";
        }

        public SetBuilder<E> setAverageElement(E averageElement) {
            this.averageElement = averageElement;
            return this;
        }

        @Override
        protected SetBuilder<E> self() {
            return this;
        }

        public ChronicleSet<E> build() {
            // ChronicleSet构建器
            ChronicleSetBuilder<E> setBuilder = ChronicleSetBuilder
                    // 设置ElementType
                    .of(elementType)
                    // 设置名称
                    .name(name)
                    // 设置条目总数
                    .entries(entries);

            // 设置块大小
            if (actualChunkSize > 0)
                setBuilder.actualChunkSize(actualChunkSize);
            // 设置关闭操作
            if (preShutdownAction != null)
                setBuilder.setPreShutdownAction(preShutdownAction);
            // 基于元素值设置平均长度
            if (averageElement != null)
                setBuilder.averageKey(averageElement);

            if (isPersistent) {
                checkSaveFile();
                try {
                    // Is recover data
                    return isRecover ? setBuilder.recoverPersistedTo(saveFile, true)
                            : setBuilder.createPersistedTo(saveFile);
                } catch (IOException e) {
                    throw new ChronicleIOException(e);
                }
            } else {
                // 设置条目数校验
                setBuilder.checksumEntries(isChecksumEntries);
                return setBuilder.create();
            }
        }

    }

}
