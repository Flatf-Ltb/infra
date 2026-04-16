package io.flatf.persistence.chronicle.hash;

import io.flatf.common.collections.keeper.FilesKeeper;
import io.flatf.common.file.PermissionDeniedException;
import io.flatf.common.lang.Validator;
import io.flatf.persistence.chronicle.exception.ChronicleIOException;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;

@ThreadSafe
public class ChronicleSetKeeper<E> extends FilesKeeper<String, ChronicleSet<E>> {

    private final ChronicleSetConfigurator<E> cfg;

    public ChronicleSetKeeper(@Nonnull ChronicleSetConfigurator<E> cfg) {
        Validator.nonNull(cfg, "cfg");
        this.cfg = cfg;
    }

    @Nonnull
    @Override
    public ChronicleSet<E> acquire(@Nonnull String filename) throws ChronicleIOException {
        return super.acquire(filename);
    }

    @Override
    protected ChronicleSet<E> createWithKey(String filename) {
        ChronicleSetBuilder<E> builder = ChronicleSetBuilder.of(cfg.getElementClass()).entries(cfg.getEntries());
        if (cfg.getActualChunkSize() > 0)
            builder.actualChunkSize(cfg.getActualChunkSize());
        if (cfg.getAverageElement() != null)
            builder.averageKey(cfg.getAverageElement());
        if (cfg.isPersistent()) {
            File mapFile = new File(cfg.getSavePath(), filename);
            try {
                if (!mapFile.exists()) {
                    // 创建文件目录
                    createFile(mapFile);
                    return builder.createPersistedTo(mapFile);
                } else {
                    // Is recover data
                    if (cfg.isRecover())
                        return builder.recoverPersistedTo(mapFile, true);
                    else
                        return builder.createPersistedTo(mapFile);
                }
            } catch (IOException | PermissionDeniedException e) {
                throw new ChronicleIOException(e);
            }
        } else
            return builder.create();
    }

    @Override
    public void close() throws IOException {

    }

}
