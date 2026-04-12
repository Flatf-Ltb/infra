package io.flatf.persistence.chronicle;

import io.flatf.common.collections.Capacity;
import io.flatf.common.sys.SysProperties;
import io.flatf.common.thread.Sleep;
import io.flatf.persistence.chronicle.hash.ChronicleMapConfigurator;
import io.flatf.persistence.chronicle.hash.ChronicleMapKeeperOfDate;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class ChronicleMapTest {

	@Ignore
	@Test
	public void test0() {

		ChronicleMapConfigurator<String, byte[]> options = ChronicleMapConfigurator
				.newBuilder(String.class, byte[].class, SysProperties.USER_HOME, "test")
				.entries(Capacity.HEX_1_000).averageKey(new String(new byte[32])).averageValue(new byte[128])
				.build();

		try (ChronicleMapKeeperOfDate<String, byte[]> mapKeeper = new ChronicleMapKeeperOfDate<>(options)) {
			ChronicleMap<String, byte[]> acquire = mapKeeper.acquire("2019.10.11");
			while (true) {
				System.out.println(acquire.size());
				Sleep.millis(2000);
			}
		} catch (IOException ignored) {
		}

	}

}
