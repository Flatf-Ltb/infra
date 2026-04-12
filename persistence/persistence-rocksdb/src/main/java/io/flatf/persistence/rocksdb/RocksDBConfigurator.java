package io.flatf.persistence.rocksdb;

import io.flatf.common.sys.SysProperties;
import org.rocksdb.Options;
import org.rocksdb.WALRecoveryMode;

import javax.annotation.concurrent.Immutable;
import java.io.File;

import static io.flatf.common.util.StringSupport.fixPath;

@Immutable
public final class RocksDBConfigurator {

    private final String rootPath;

    private final String folder;

    private final File savePath;

    private final Options options;

    private RocksDBConfigurator(Builder builder) {
        this.rootPath = builder.rootPath;
        this.folder = builder.folder;
        this.savePath = new File(rootPath + FixedFolder + folder);
        this.options = new Options();
        options.setCreateIfMissing(builder.createIfMissing);
        options.setCreateMissingColumnFamilies(builder.createMissingColumnFamilies);
        options.setWalRecoveryMode(builder.walRecoveryMode);
    }

    private static final String FixedFolder = "rocksdb/";

    public String rootPath() {
        return rootPath;
    }

    public String folder() {
        return folder;
    }

    public File savePath() {
        return savePath;
    }

    public Options options() {
        return options;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String rootPath = SysProperties.JAVA_IO_TMPDIR;
        private String folder = "rocksdb/";

        private boolean createIfMissing = true;
        private boolean createMissingColumnFamilies = false;
        private WALRecoveryMode walRecoveryMode = WALRecoveryMode.PointInTimeRecovery;

        private Builder() {
        }

        public Builder rootPath(String rootPath) {
            this.rootPath = fixPath(rootPath);
            return this;
        }

        public Builder folder(String folder) {
            this.folder = fixPath(folder);
            return this;
        }

        public Builder createIfMissing(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }

        public Builder createMissingColumnFamilies(boolean createMissingColumnFamilies) {
            this.createMissingColumnFamilies = createMissingColumnFamilies;
            return this;
        }

        public Builder walRecoveryMode(WALRecoveryMode walRecoveryMode) {
            this.walRecoveryMode = walRecoveryMode;
            return this;
        }

        public RocksDBConfigurator build() {
            return new RocksDBConfigurator(this);
        }

    }

}
