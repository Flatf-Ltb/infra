package io.flatf.serialization.avro;

import io.flatf.common.sys.SysProperties;
import io.flatf.serialization.avro.msg.AvroTextMsg;
import io.flatf.serialization.avro.msg.ContentType;
import io.flatf.serialization.avro.msg.Envelope;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class WriteAsAvro {

	public static void main(String[] args) throws Exception {
		int sequence = 0;

		Envelope envelope = new Envelope();
		envelope.setCode(1);
		envelope.setVersion(1);
		envelope.setContentType(ContentType.STRING);
		AvroTextMsg msg0 = new AvroTextMsg();
		msg0.setEnvelope(envelope);
		msg0.setEpoch(System.currentTimeMillis());
		msg0.setSequence(++sequence);
		msg0.setContent("TEXT0");

		AvroTextMsg msg1 = AvroTextMsg.newBuilder().setEnvelope(envelope).setEpoch(System.currentTimeMillis())
				.setSequence(++sequence).setContent("TEXT1").build();

		// Serializing

		// Now let's serialize our Users to disk.

		// 把两条记录写入到文件中 (序列化)
		// DatumWriter是一个接口,需要用它的实现类创建对象
		// 如果创建了实例化对象, 则使用SpecificDatumWriter来创建对象
		// 如果没有创建实例化对象, 则使用GenericDatumWriter来创建对象
		DatumWriter<AvroTextMsg> writer = new SpecificDatumWriter<AvroTextMsg>();
		DataFileWriter<AvroTextMsg> fileWriter = new DataFileWriter<AvroTextMsg>(writer);

		// 创建序列化文件, 文件会创建到项目的根目录下
		fileWriter.create(AvroTextMsg.getClassSchema(), new File(SysProperties.USER_HOME_FILE, "test.avro"));
		fileWriter.setCodec(CodecFactory.snappyCodec());

		// 写入内容
		fileWriter.append(msg0);
		fileWriter.append(msg1);

		fileWriter.flush();
		fileWriter.close();

	}

}
