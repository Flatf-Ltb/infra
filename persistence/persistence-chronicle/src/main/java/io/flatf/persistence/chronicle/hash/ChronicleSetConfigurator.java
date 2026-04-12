package io.flatf.persistence.chronicle.hash;

import io.flatf.common.collections.Capacity;
import io.flatf.common.config.Configurator;
import io.flatf.common.datetime.DateTimeUtil;
import io.flatf.common.lang.Validator;
import io.flatf.common.sys.SysProperties;

import javax.annotation.Nonnull;
import java.io.File;

import static io.flatf.common.util.StringSupport.fixPath;

public final class ChronicleSetConfigurator<E> implements Configurator {

    private final Class<E> elementClass;
    private final E averageElement;

    private final boolean recover;
    private final boolean persistent;

    private final long entries;
    private final int actualChunkSize;

    private final String rootPath;
    private final String folder;

    // final save path
    private final File savePath;

    private final String cfgInfo;

    public Class<E> getElementClass() {
        return elementClass;
    }

    public E getAverageElement() {
        return averageElement;
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

    private ChronicleSetConfigurator(Builder<E> builder) {
        this.elementClass = builder.elementClass;
        this.averageElement = builder.averageElement;
        this.recover = builder.recover;
        this.persistent = builder.persistent;
        this.entries = builder.entries;
        this.actualChunkSize = builder.actualChunkSize;
        this.rootPath = builder.rootPath;
        this.folder = builder.folder;
        this.savePath = new File(rootPath + "chronicle-set/" + folder);
        this.cfgInfo = "[SavedFile==" + savePath.getAbsolutePath() + "]:" +
                ":[ElementType==" + elementClass.getSimpleName() + "]";
    }

    public static <E> Builder<E> newBuilder(@Nonnull Class<E> elementClass) {
        return newBuilder(elementClass, SysProperties.JAVA_IO_TMPDIR,
                "auto-create-" + DateTimeUtil.datetimeOfSecond());
    }

    public static <E> Builder<E> newBuilder(@Nonnull Class<E> elementClass,
                                         @Nonnull String folder) {
        return newBuilder(elementClass, SysProperties.JAVA_IO_TMPDIR, folder);
    }

    public static <E> Builder<E> newBuilder(@Nonnull Class<E> elementClass,
                                         @Nonnull String rootPath,
                                         @Nonnull String folder) {
        Validator.nonNull(elementClass, "elementClass");
        Validator.nonNull(rootPath, "rootPath");
        Validator.nonNull(folder, "folder");
        return new Builder<>(elementClass, rootPath, folder);
    }

    @Override
    public String getConfigInfo() {
        return cfgInfo;
    }

    /**
     * @param <E>
     * @author yellow013
     */
    public static class Builder<E> {

        private final Class<E> elementClass;
        private final String rootPath;
        private final String folder;

        private E averageElement;

        private boolean recover = false;
        private boolean persistent = true;

        private long entries = 4096;
        private int actualChunkSize;

        private Builder(@Nonnull Class<E> elementClass, @Nonnull String rootPath, @Nonnull String folder) {
            this.elementClass = elementClass;
            this.rootPath = fixPath(rootPath);
            this.folder = fixPath(folder);
        }

        public Builder<E> averageElement(E averageElement) {
            this.averageElement = averageElement;
            return this;
        }

        public Builder<E> enableRecover() {
            this.recover = true;
            return this;
        }

        public Builder<E> persistent(boolean persistent) {
            this.persistent = persistent;
            return this;
        }

        public Builder<E> actualChunkSize(int actualChunkSize) {
            this.actualChunkSize = actualChunkSize;
            return this;
        }

        public Builder<E> entries(long entries) {
            this.entries = entries;
            return this;
        }

        public Builder<E> entriesOfPow2(Capacity capacity) {
            this.entries = capacity.size();
            return this;
        }

        public ChronicleSetConfigurator<E> build() {
            return new ChronicleSetConfigurator<>(this);
        }
    }

}