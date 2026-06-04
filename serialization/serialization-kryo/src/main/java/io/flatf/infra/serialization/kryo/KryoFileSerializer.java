package io.flatf.infra.serialization.kryo;

import io.flatf.common.serialization.specific.FileSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Collection;

public class KryoFileSerializer<T> implements FileSerializer<Collection<T>> {

    @Override
    public File serialize(@Nonnull Collection<T> source) {
        // TODO Auto-generated method stub
        return null;
    }

}
