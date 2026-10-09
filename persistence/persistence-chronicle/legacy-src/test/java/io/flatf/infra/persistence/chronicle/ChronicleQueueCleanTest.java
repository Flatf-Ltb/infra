package io.flatf.infra.persistence.chronicle;

import java.time.ZonedDateTime;

import org.junit.Test;

import io.flatf.common.constant.TimeZoneConst;
import io.flatf.common.thread.Sleep;
import io.flatf.infra.persistence.chronicle.queue.ChronicleStringQueue;
import io.flatf.infra.persistence.chronicle.queue.ChronicleStringQueue.ChronicleStringAppender;
import io.flatf.infra.persistence.chronicle.queue.ChronicleStringQueue.ChronicleStringReader;
import io.flatf.infra.persistence.chronicle.queue.FileCycle;

public class ChronicleQueueCleanTest {

	// @Ignore
	@Test
	public void test0() {

		ChronicleStringQueue persistence = ChronicleStringQueue.newBuilder().folder("test").fileClearCycle(5)
				.fileCycle(FileCycle.FIVE_MINUTELY).build();

		ChronicleStringAppender appender = persistence.acquireAppender();
		ChronicleStringReader reader = persistence.createReader(System.out::println);

		// boolean moved = reader.moveTo(LocalDateTime.now().minusMinutes(20),
		// TimeZones.SYSTEM_DEFAULT);

		// System.out.println("is moved == " + moved);
		reader.runWithNewThread();
		int i = 0;
		while (true) {
			try {
				appender.append(ZonedDateTime.now(TimeZoneConst.UTC).toString());
				Thread.sleep(2000);
				i++;
				if (i == 20) {
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		persistence.close();
		Sleep.millis(2000);
		System.out.println(appender.isClosed());
		System.out.println(reader.isClosed());

	}

}
