package io.flatf.foundation.transport.rmq.config;

import lombok.Getter;

/**
 * @author yellow013
 */
public sealed abstract class RmqConfig permits RmqProducerConfig, RmqConsumerConfig {

    /**
     * 连接配置信息
     */
    @Getter
    private final RmqConnectionConfig connectionConfig;

    protected RmqConfig(RmqConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

}
