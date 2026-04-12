package io.flatf.persistence.chronicle.hash;

import io.flatf.common.collections.Capacity;
import io.flatf.common.config.Configurator;
import io.flatf.common.datetime.DateTimeUtil;
import io.flatf.common.lang.Validator;
import io.flatf.common.sys.SysProperties;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.io.File;

import static io.flatf.common.util.StringSupport.fixPath;

@Immutable
public final class ChronicleMapConfigurator<K, V> implements Configurator {

    private final Class<K> keyClass;
    private final Class<V> valueClass;

    private final K averageKey;
    private final V averageValue;

    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;
    private final boolean recover;
    private final boolean persistent;

    private final long entries;
    private final int actualChunkSize;

    private final String rootPath;
    private final String folder;

    // final save path
    private final File savePath;

    private final String cfgInfo;

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public K getAverageKey() {
        return averageKey;
    }

    public V getAverageValue() {
        return averageValue;
    }

    public boolean isPutReturnsNull() {
        return putReturnsNull;
    }

    public boolean isRemoveReturnsNull() {
        return removeReturnsNull;
    }

    public boolean isRecover() {
        return recover;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public long getEntries() {
        return entries;
    }

    public int getActualChunkSize() {
        return actualChunkSize;
    }

    public String getRootPath() {
        return rootPath;
    }

    public String getFolder() {
        return folder;
    }

    public File getSavePath() {
        return savePath;
    }

    // extended use
    private final Builder<K, V> builder;

    private ChronicleMapConfigurator(Builder<K, V> builder) {
        this.builder = builder;
        this.keyClass = builder.keyClass;
        this.valueClass = builder.valueClass;
        this.averageKey = builder.averageKey;
        this.averageValue = builder.averageValue;
        this.putReturnsNull = builder.putReturnsNull;
        this.removeReturnsNull = builder.removeReturnsNull;
        this.recover = builder.recover;
        this.persistent = builder.persistent;
        this.entries = builder.entries;
        this.actualChunkSize = builder.actualChunkSize;
        this.rootPath = builder.rootPath;
        this.folder = builder.folder;
        this.savePath = new File(rootPath + "chronicle-map/" + folder);
        this.cfgInfo = "[SavedFile==" + savePath.getAbsolutePath() + "]:" +
                ":[KeyType==" + keyClass.getSimpleName() + ",ValueType==" + valueClass.getSimpleName() + "]";
    }

    /**
     * @param <K>        K type
     * @param <V>        V type
     * @param keyClass   K Class
     * @param valueClass V Class
     * @return Builder<K, V>
     * @throws NullPointerException npe
     */
    public static <K, V> Builder<K, V> newBuilder(@Nonnull Class<K> keyClass,
                                                  @Nonnull Class<V> valueClass)
            throws NullPointerException {
        return newBuilder(keyClass, valueClass, SysProperties.JAVA_IO_TMPDIR,
                "auto-create-folder-" + DateTimeUtil.datetimeOfSecond());
    }

    /**
     * @param <K>        K type
     * @param <V>        V type
     * @param keyClass   K Class
     * @param valueClass V Class
     * @param folder     String
     * @return Builder<K, V>
     * @throws NullPointerException npe
     */
    public static <K, V> Builder<K, V> newBuilder(@Nonnull Class<K> keyClass,
                                                  @Nonnull Class<V> valueClass,
                                                  @Nonnull String folder)
            throws NullPointerException {
        return newBuilder(keyClass, valueClass, SysProperties.JAVA_IO_TMPDIR, folder);
    }

    /**
     * @param <K>        K type
     * @param <V>        V type
     * @param keyClass   K Class
     * @param valueClass V Class
     * @param rootPath   String
     * @param folder     String
     * @return Builder<K, V>
     * @throws NullPointerException npe
     */
    public static <K, V> Builder<K, V> newBuilder(@Nonnull Class<K> keyClass,
                                                  @Nonnull Class<V> valueClass,
                                                  @Nonnull String rootPath,
                                                  @Nonnull String folder)
            throws NullPointerException {
        Validator.nonNull(keyClass, "keyClass");
        Validator.nonNull(valueClass, "valueClass");
        Validator.nonNull(rootPath, "rootPath");
        Validator.nonNull(folder, "folder");
        return new Builder<>(keyClass, valueClass, rootPath, folder);
    }

    /**
     * @param <K>      K type
     * @param <V>      V type
     * @param original ChronicleMapConfigurator<K, V>
     * @return Builder<K, V>
     * @throws NullPointerException npe
     */
    public static <K, V> Builder<K, V> reset(@Nonnull ChronicleMapConfigurator<K, V> original)
            throws NullPointerException {
        Validator.nonNull(original, "original");
        return original.builder;
    }

    @Override
    public String getConfigInfo() {
        return cfgInfo;
    }

    /**
     * @param <K>
     * @param <V>
     * @author yellow013
     */
    public static class Builder<K, V> {

        private final Class<K> keyClass;
        private final Class<V> valueClass;
        private final String rootPath;
        private final String folder;

        private K averageKey;
        private V averageValue;

        private boolean putReturnsNull = false;
        private boolean removeReturnsNull = false;
        private boolean recover = false;
        private boolean persistent = true;

        private long entries = 4096;
        private int actualChunkSize;

        private Builder(@Nonnull Class<K> keyClass,
                        @Nonnull Class<V> valueClass,
                        @Nonnull String rootPath,
                        @Nonnull String folder) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.rootPath = fixPath(rootPath);
            this.folder = fixPath(folder);
        }

        /**
         * @param averageKey K
         * @return Builder<K, V>
         */
        public Builder<K, V> averageKey(K averageKey) {
            this.averageKey = averageKey;
            return this;
        }

        /**
         * @param averageValue V
         * @return Builder<K, V>
         */
        public Builder<K, V> averageValue(V averageValue) {
            this.averageValue = averageValue;
            return this;
        }

        /**
         * @return Builder<K, V>
         */
        public Builder<K, V> enablePutReturnsNull() {
            this.putReturnsNull = true;
            return this;
        }

        /**
         * @return Builder<K, V>
         */
        public Builder<K, V> enableRemoveReturnsNull() {
            this.removeReturnsNull = true;
            return this;
        }

        /**
         * @return Builder<K, V>
         */
        public Builder<K, V> enableRecover() {
            this.recover = true;
            return this;
        }

        /**
         * @param persistent boolean
         * @return Builder<K, V>
         */
        public Builder<K, V> persistent(boolean persistent) {
            this.persistent = persistent;
            return this;
        }

        /**
         * @param actualChunkSize int
         * @return Builder<K, V>
         */
        public Builder<K, V> actualChunkSize(int actualChunkSize) {
            this.actualChunkSize = actualChunkSize;
            return this;
        }

        /**
         * @param entries long
         * @return Builder<K, V>
         */
        public Builder<K, V> entries(long entries) {
            this.entries = entries;
            return this;
        }

        /**
         * @param capacity Capacity
         * @return Builder<K, V>
         */
        public Builder<K, V> entries(Capacity capacity) {
            this.entries = capacity.size();
            return this;
        }

        /**
         * @return ChronicleMapConfigurator<K, V>
         */
        public ChronicleMapConfigurator<K, V> build() {
            return new ChronicleMapConfigurator<>(this);
        }
    }

    public static void main(String[] args) {

        ChronicleMapConfigurator<String, Long> configurator = ChronicleMapConfigurator
                .newBuilder(String.class, Long.class, SysProperties.USER_HOME, "/user").build();

        System.out.println(configurator.getConfigInfo());

    }

}