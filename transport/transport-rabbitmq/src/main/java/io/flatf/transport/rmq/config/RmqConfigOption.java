package io.flatf.transport.rmq.config;

import io.flatf.common.config.ConfigOption;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum RmqConfigOption implements ConfigOption {

    HOST("rmq.host", "rabbitmq.host"),

    PORT("rmq.port", "rabbitmq.port"),

    USERNAME("rmq.username", "rabbitmq.username"),

    PASSWORD("rmq.password", "rabbitmq.password"),

    VIRTUAL_HOST("rmq.virtualHost", "rabbitmq.virtualHost");

    @Getter
    private final String configName;

    @Getter
    private final String otherName;

}
