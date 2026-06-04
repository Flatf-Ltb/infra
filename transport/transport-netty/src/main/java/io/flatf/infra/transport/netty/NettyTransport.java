package io.flatf.infra.transport.netty;

import io.flatf.common.annotation.AbstractFunction;
import io.flatf.common.lang.Validator;
import io.flatf.foundation.transport.TransportComponent;
import io.flatf.foundation.transport.api.Transport;
import io.flatf.infra.transport.netty.configurator.NettyConfig;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import static io.flatf.common.sys.CurrentRuntime.availableProcessors;

public abstract class NettyTransport extends TransportComponent implements Transport {

    protected final String tag;
    protected final NettyConfig configurator;

    protected final ChannelHandler[] handlers;
    protected final EventLoopGroup workerGroup;

    /**
     * @param tag          String
     * @param configurator NettyConfigurator
     * @param handlers     ChannelHandler[]
     */
    protected NettyTransport(String tag, NettyConfig configurator, ChannelHandler... handlers) {
        Validator.nonNull(configurator, "configurator");
        Validator.requiredLength(handlers, 1, "handlers");
        this.tag = tag;
        this.configurator = configurator;
        this.handlers = handlers;
        this.workerGroup = new NioEventLoopGroup(availableProcessors() * 2 - availableProcessors() / 2);
        init();
        newStartTime();
    }

    @AbstractFunction
    protected abstract void init();

    @Override
    public String getName() {
        return tag;
    }

}
