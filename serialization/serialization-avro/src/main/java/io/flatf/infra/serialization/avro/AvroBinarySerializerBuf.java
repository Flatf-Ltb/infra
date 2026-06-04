package io.flatf.infra.serialization.avro;

import io.flatf.common.concurrent.map.NonBlockingMaps;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.serialization.specific.ByteBufferSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;

@ThreadSafe
public final class AvroBinarySerializerBuf<T extends SpecificRecord> implements ByteBufferSerializer<T> {

    private static final Logger log = Log4j2LoggerFactory.getLogger(AvroBinarySerializerBuf.class);

    private final Class<T> type;

    private final NonBlockingHashMapLong<AvroBinarySerializer<T>> serializers = NonBlockingMaps.newLongHashMap(16);

    private final int bufSize;

    /**
     * Use default ByteArrayOutputStream size : 8192
     */
    public AvroBinarySerializerBuf(Class<T> type) {
        this(type, 8192);
    }

    /**
     * @param type    : object type
     * @param bufSize : size is inner ByteArrayOutputStream size
     */
    public AvroBinarySerializerBuf(Class<T> type, int bufSize) {
        this.type = type;
        this.bufSize = bufSize;
    }

    @Override
    public ByteBuffer serialize(@Nonnull T obj) {
        try {
            return getSerializer().serialize(obj);
        } catch (Exception e) {
            log.error("serialization func -> {}", e.getMessage(), e);
            throw e;
        }
    }

    private AvroBinarySerializer<T> getSerializer() {
        long threadId = Thread.currentThread().threadId();
        return serializers.putIfAbsent(threadId, new AvroBinarySerializer<>(type, bufSize));
    }

}
