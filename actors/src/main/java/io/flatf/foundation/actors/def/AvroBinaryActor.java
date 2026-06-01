package io.flatf.foundation.actors.def;

import io.flatf.foundation.serialization.avro.msg.AvroBinaryMsg;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class AvroBinaryActor extends BaseActorT1<ByteBuffer> {

    @Override
    protected Class<ByteBuffer> eventType() {
        return ByteBuffer.class;
    }

    @Override
    protected final void onEvent(ByteBuffer binary) {
        try {
            AvroBinaryMsg msg = AvroBinaryMsg.fromByteBuffer(binary);
            handleMsg(msg);
        } catch (IOException e) {
            log.error("Binary deserialization throw IOException, message -> {}, binary.capacity=={}",
                    e.getMessage(), binary.capacity(), e);
        }
    }

    protected abstract void handleMsg(AvroBinaryMsg msg);

}

