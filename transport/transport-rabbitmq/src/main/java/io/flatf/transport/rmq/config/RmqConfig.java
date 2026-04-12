package io.flatf.transport.rmq.config;

import lombok.Getter;

/**
 * @author yellow013
 */
public abstract class RmqConfig {

    /**
     * 连接配置信息
     */
    @Getter
    private final RmqConnection connection;

    protected RmqConfig(RmqConnection connection) {
        this.connection = connection;
    }

}
