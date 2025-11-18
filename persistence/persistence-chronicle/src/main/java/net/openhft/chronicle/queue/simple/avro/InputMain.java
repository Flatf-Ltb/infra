package net.openhft.chronicle.queue.simple.avro;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

public class InputMain {

	public static void main(String[] args) throws IOException {

		AvroHelper avro = new AvroHelper();

		String path = "avro-queue";
		RollingChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
		ExcerptAppender appender = queue.createAppender();

		try (DocumentContext dc = appender.writingDocument()) {
			GenericRecord user = avro.getGenericRecord();
			user.put("name", "Alyssa");
			user.put("favorite_number", 256);
			avro.writeToOS(user, dc.wire().bytes().outputStream());
		}

		try (DocumentContext dc = appender.writingDocument()) {
			GenericRecord user = avro.getGenericRecord();
			user.put("name", "Ben");
			user.put("favorite_number", 7);
			user.put("favorite_color", "red");
			avro.writeToOS(user, dc.wire().bytes().outputStream());
		}

		System.out.println("2 records written.");
	}
	
}
