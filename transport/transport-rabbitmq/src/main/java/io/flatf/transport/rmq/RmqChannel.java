package io.flatf.transport.rmq;

import com.rabbitmq.client.Channel;
import io.flatf.transport.rmq.config.RmqConnectionConfig;

import java.time.ZonedDateTime;

import static io.flatf.common.constant.TimeZoneConst.SYS_DEFAULT;

public final class RmqChannel extends RmqTransport {

    /**
     * Create GeneralChannel of host, port, username and password
     *
     * @param host     String
     * @param port     int
     * @param username String
     * @param password String
     * @return RmqChannel
     */
    public static RmqChannel with(String host, int port, String username, String password) {
        return with(RmqConnectionConfig.with(host, port, username, password).build());
    }

    /**
     * Create GeneralChannel of host, port, username, password and virtualHost
     *
     * @param host        String
     * @param port        int
     * @param username    String
     * @param password    String
     * @param virtualHost String
     * @return RmqChannel
     */
    public static RmqChannel with(String host, int port, String username, String password, String virtualHost) {
        return with(RmqConnectionConfig.with(host, port, username, password, virtualHost).build());
    }

    /**
     * Create GeneralChannel of RmqConnection
     *
     * @param connection io.flatf.transport.rmq.configurator.RmqConnection
     * @return RmqChannel
     */
    public static RmqChannel with(RmqConnectionConfig connection) {
        return new RmqChannel("RmqChannel-" + ZonedDateTime.now(SYS_DEFAULT), connection);
    }

    /**
     * Create GeneralChannel of Channel
     *
     * @param channel com.rabbitmq.client.Channel
     * @return RmqChannel
     */
    public static RmqChannel newWith(Channel channel) {
        return new RmqChannel(channel);
    }

    /**
     * @param tag        String
     * @param connection RmqConnection
     */
    private RmqChannel(String tag, RmqConnectionConfig connection) {
        super(tag, connection);
        createConnection();
    }

    /**
     * @param channel Channel
     */
    private RmqChannel(Channel channel) {
        super("channel-" + channel.getChannelNumber());
        this.channel = channel;
    }

    /**
     * @return Channel
     */
    public Channel internalChannel() {
        return channel;
    }

}

