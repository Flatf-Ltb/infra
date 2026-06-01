package io.flatf.foundation.serialization.avro;

import io.flatf.foundation.common.sys.SysProperties;
import io.flatf.foundation.serialization.avro.msg.AvroTextMsg;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;

public class ReadFromAvro {

	public static void main(String[] args) throws Exception {

		File file = new File(SysProperties.USER_HOME_FILE, "test");

		final DatumReader<AvroTextMsg> reader = new SpecificDatumReader<>();

		final DataFileReader<AvroTextMsg> msgReader = new DataFileReader<>(file, reader);

		AvroTextMsg msg;

		while (msgReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			msg = msgReader.next();
			System.out.println(msg);
		}

		msgReader.close();
	}

}
