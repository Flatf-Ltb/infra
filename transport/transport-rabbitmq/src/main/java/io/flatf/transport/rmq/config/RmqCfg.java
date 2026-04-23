package io.flatf.transport.rmq.config;

import lombok.Getter;

/**
 * @author yellow013
 */
public sealed abstract class RmqCfg permits RmqPublisherCfg, RmqConsumerCfg {

    /**
     * 连接配置信息
     */
    @Getter
    private final RmqConnection connection;

    protected RmqCfg(RmqConnection connection) {
        this.connection = connection;
    }

}
