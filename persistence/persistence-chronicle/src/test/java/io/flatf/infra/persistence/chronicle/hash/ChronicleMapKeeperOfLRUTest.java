package io.flatf.infra.persistence.chronicle.hash;

import io.flatf.common.epoch.EpochUtil;
import io.flatf.common.log4j2.Log4j2Configurator;
import io.flatf.common.log4j2.Log4j2Configurator.LogLevel;
import io.flatf.common.sys.SysProperties;
import io.flatf.common.thread.Sleep;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.IOException;
import java.time.Duration;

public class ChronicleMapKeeperOfLRUTest {

    static {
        Log4j2Configurator.setLogLevel(LogLevel.INFO);
    }

    public static void main(String[] args) {

        ChronicleMapConfigurator.Builder<Long, String> builder = ChronicleMapConfigurator
                .newBuilder(Long.class, String.class, SysProperties.USER_HOME, "test")
                .averageValue(Long.toString(Long.MIN_VALUE)).entries(56636);

        try (ChronicleMapKeeperOfLRU<Long, String> mapKeeper = new ChronicleMapKeeperOfLRU<>(builder.build(),
                Duration.ofMinutes(3))) {
            long l = 0;
            long fileCycle = 60 * 1000;
            do {
                long millis = EpochUtil.getEpochMillis();
                long filename = millis / fileCycle;
                ChronicleMap<Long, String> acquire = mapKeeper.acquire(Long.toString(filename));
                acquire.put(++l, Long.toString(l));
                Sleep.millis(1000);
            } while (l != 300);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
