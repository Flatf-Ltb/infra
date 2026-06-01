package io.flatf.foundation.serialization.avro;

import io.flatf.foundation.common.epoch.EpochUtil;
import io.flatf.foundation.serialization.avro.msg.AvroBinaryMsg;
import io.flatf.foundation.serialization.avro.msg.ContentType;
import org.junit.Test;

import java.nio.ByteBuffer;

public class AvroBinaryDeserializerTest {

	@Test
	public void test() {
		AvroBinarySerializer<AvroBinaryMsg> serializer = new AvroBinarySerializer<>(AvroBinaryMsg.class);
		AvroBinaryDeserializer<AvroBinaryMsg> deserializer = new AvroBinaryDeserializer<>(AvroBinaryMsg.class);

		AvroBinaryMsg msg0 = AvroBinaryMsgFactory.emptyBinaryMsg();
		msg0.getEnvelope().setCode(1).setContentType(ContentType.INT).setVersion(1);
		msg0.setEpoch(EpochUtil.getEpochMillis()).setSequence(1).setContent(ByteBuffer.allocate(10));

		ByteBuffer buffer = serializer.serialize(msg0);

		msg0.setEpoch(EpochUtil.getEpochMillis() + 1000);
		System.out.println(msg0);

		AvroBinaryMsg msg = deserializer.deserialization(buffer.array());

		System.out.println(msg);

	}

}
