package io.flatf.foundation.serialization.avro;

import io.flatf.foundation.common.sys.SysProperties;
import io.flatf.foundation.serialization.avro.msg.AvroTextMsg;
import io.flatf.foundation.serialization.avro.msg.ContentType;
import io.flatf.foundation.serialization.avro.msg.Envelope;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroMsgFileWriterTest {

	@Test
	public void test() {
		List<AvroTextMsg> list = new ArrayList<>(2000);
		Envelope envelope = Envelope.newBuilder().setCode(1).setVersion(1).setContentType(ContentType.STRING).build();
		for (int i = 0; i < 2000; i++) {
			list.add(AvroTextMsg.newBuilder().setEnvelope(envelope).setEpoch(System.currentTimeMillis()).setSequence(i)
					.setContent("content->" + i).build());
		}
		File saveFile = new File(SysProperties.USER_HOME_FILE, "test");
		try (AvroFileWriter<AvroTextMsg> writer = new AvroFileWriter<>(new AvroTextMsg())) {
			writer.append(saveFile, list);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
