package io.mercury.common.config;

import static io.mercury.common.lang.ThrowsUtil.throwsUnsupportedOperation;

public final class ConfigStorage {

    private ConfigStorage() {
        throwsUnsupportedOperation("[ConfigStorage] cannot be instantiated");
    }

}
