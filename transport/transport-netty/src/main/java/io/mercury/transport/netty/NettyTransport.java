package io.mercury.transport.netty;

import io.mercury.common.annotation.AbstractFunction;
import io.mercury.common.lang.Validator;
import io.mercury.transport.TransportComponent;
import io.mercury.transport.api.Transport;
import io.mercury.transport.netty.configurator.NettyCfg;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import static io.mercury.common.sys.CurrentRuntime.availableProcessors;

public abstract class NettyTransport extends TransportComponent implements Transport {

    protected final String tag;
    protected final NettyCfg configurator;

    protected final ChannelHandler[] handlers;
    protected final EventLoopGroup workerGroup;

    /**
     * @param tag          String
     * @param configurator NettyConfigurator
     * @param handlers     ChannelHandler[]
     */
    protected NettyTransport(String tag, NettyCfg configurator, ChannelHandler... handlers) {
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
