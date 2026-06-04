package io.flatf.infra.configuration.nacos;

import io.flatf.common.log4j2.Log4j2Configurator;
import io.flatf.common.log4j2.Log4j2Configurator.LogLevel;
import io.flatf.common.util.PropertiesUtil;
import org.junit.Test;

import java.util.Properties;

public class NacosReaderTest {

    static {
        Log4j2Configurator.setLogLevel(LogLevel.INFO);
    }

    @Test
    public void test() {
        Properties prop = NacosReader
                .getProperties("", "", "");
        PropertiesUtil.show(prop);
    }

}
