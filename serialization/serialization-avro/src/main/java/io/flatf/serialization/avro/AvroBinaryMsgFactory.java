package io.flatf.serialization.avro;

import com.lmax.disruptor.EventFactory;
import io.flatf.serialization.avro.msg.AvroBinaryMsg;
import io.flatf.serialization.avro.msg.ContentType;
import io.flatf.serialization.avro.msg.Envelope;

import java.nio.ByteBuffer;

/**
 * AvroBinaryMsgFactory implements EventFactory
 *
 * @author yellow013
 */
public class AvroBinaryMsgFactory implements EventFactory<AvroBinaryMsg> {

    
    public static final AvroBinaryMsgFactory INSTANCE = new AvroBinaryMsgFactory();

    @Override
    public AvroBinaryMsg newInstance() {
        return emptyBinaryMsg();
    }

    private static final ByteBuffer EMPTY_BUF = ByteBuffer.allocate(0);

    public static Envelope emptyEnvelope() {
        return Envelope.newBuilder().setCode(Integer.MIN_VALUE).setContentType(ContentType.NULL)
                .setVersion(Integer.MIN_VALUE).build();
    }

    public static AvroBinaryMsg emptyBinaryMsg() {
        return AvroBinaryMsg.newBuilder().setEnvelope(emptyEnvelope()).setEpoch(0).setSequence(0).setContent(EMPTY_BUF)
                .build();
    }

}
