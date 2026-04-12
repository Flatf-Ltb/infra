package io.flatf.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;

/**
 * @author yellow013
 */
public final class ConfigUtil {

    public static File findConfigFileAtHome(@Nonnull final String filename) {
        return null;
    }

    /**
     * @param config Config
     */
    public static void showConfig(@Nonnull Config config) {
        showConfig(config, null);
    }

    /**
     * @param config Config
     * @param log    Logger
     */
    public static void showConfig(@Nonnull Config config, @Nullable Logger log) {
        config.entrySet().forEach(entry -> {
            ConfigValue value = entry.getValue();
            if (log != null)
                log.info("Key -> [{}],  ValueType -> [{}],  Value -> [{}]",
                        entry.getKey(), value.valueType(), value.unwrapped());
            else
                System.out.println("Key -> [" + entry.getKey() + "],  ValueType -> [" + value.valueType() + "],  Value -> [" + value.unwrapped() + "]");
        });
    }

}
