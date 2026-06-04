package io.flatf.infra.transport.netty;

import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.infra.api.TransportServer;
import io.flatf.infra.transport.netty.configurator.NettyConfig;
import io.flatf.infra.transport.netty.handler.GeneralNettyHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;

public class NettyServer extends NettyTransport implements TransportServer {

    private static final Logger log = Log4j2LoggerFactory.getLogger(NettyServer.class);

    private EventLoopGroup bossGroup;
    private ServerBootstrap bootstrap;

    /**
     * @param tag          String
     * @param configurator NettyConfigurator
     * @param handlers     ChannelHandler[]
     */
    public NettyServer(String tag, NettyConfig configurator, ChannelHandler... handlers) {
        super(tag, configurator, handlers);
    }

    @Override
    protected void init() {
        this.bossGroup = new NioEventLoopGroup();
        this.bootstrap = new ServerBootstrap();
        this.bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        channel.pipeline().addLast(handlers);
                    }
                }).option(ChannelOption.SO_BACKLOG, configurator.backlog())
                .childOption(ChannelOption.SO_KEEPALIVE, configurator.keepAlive())
                .childOption(ChannelOption.TCP_NODELAY, configurator.tcpNoDelay());
        log.info("{} : Init-ServerBootStrap.bind -> {}", tag, configurator.connectionInfo());
    }

    @Override
    public void bind() {
        try {
            // Create a new Channel and bind it.
            bootstrap.bind(configurator.getHost(), configurator.getPort()).sync()
                    // Returns a channel where the I/O operation associated with this future takes
                    // place.
                    .channel()
                    // Returns the ChannelFuture which will be notified when this channel is closed.
                    // This method always returns the same future instance.
                    .closeFuture()
                    // Waits for this future until it is done, and rethrows the cause of the failure
                    // if this future failed.
                    .sync();
        } catch (InterruptedException e) {
            log.error("NettyServer method startup() -> {}", e.getMessage(), e);
            closeIgnoreException();
        }
    }

    @Override
    public boolean closeIgnoreException() {
        log.info("NettyServer call method destroy().");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        return true;
    }

    @Override
    public boolean isConnected() {
        return bossGroup.isShutdown() && workerGroup.isShutdown();
    }

    public static void main(String[] args) throws Exception {

        NettyConfig configurator = NettyConfig.builder("192.168.1.138", 7901).build();

        try (NettyServer nettyServer = new NettyServer("LocalTest", configurator, new GeneralNettyHandler() {

            @Override
            public void handlerAdded(ChannelHandlerContext ctx) {
                sendBytes(ctx, "hello".getBytes());
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                byte[] recvBytes = getRecvBytes((ByteBuf) msg);
                System.out.println(new String(recvBytes));
            }

        })) {
            nettyServer.bind();
        }

    }

}
