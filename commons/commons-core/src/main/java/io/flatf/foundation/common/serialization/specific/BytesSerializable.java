package io.flatf.foundation.common.serialization.specific;

import javax.annotation.Nonnull;

public interface BytesSerializable {

    @Nonnull
    byte[] toBytes();

}
