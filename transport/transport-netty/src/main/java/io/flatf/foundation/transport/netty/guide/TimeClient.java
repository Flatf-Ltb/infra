package io.flatf.foundation.transport.netty.guide;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * 编写服务端和客户端最大的并且唯一的不同是使用不同的 BootStrap 和 Channel 的实现.
 *
 * @author yellow013
 */
public class TimeClient {

    private final String host;
    private final int port;

    private TimeClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap(); // (#)
            /*
             * #) BootStrap 和 ServerBootstrap 类似.
             *
             * 是对非服务端的 channel 而言, 比如客户端或者无连接传输模式的 channel
             */
            bootstrap.group(workerGroup); // (#)
            /*
             * #) 如果只指定了一个 EventLoopGroup, 就会即作为一个 boss group, 也会作为一个 worker group,
             * 尽管客户端不需要使用到 boss worker.
             */
            bootstrap.channel(NioSocketChannel.class); // (#)
            /*
             * #) 代替 NioServerSocketChannel 的是 NioSocketChannel, 这个类在客户端channel 被创建时使用.
             */
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true); // (#)
            /*
             * #) 不像在使用 ServerBootstrap 时需要用 childOption() 方法, 因为客户端的 SocketChannel 没有父亲.
             */
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new TimeClientHandler());
                }
            });
            // 启动客户端
            ChannelFuture future = bootstrap.connect(host, port).sync(); // (#)
            /*
             * #) 使用 connect() 方法代替 bind() 方法.
             */
            // 等待连接关闭
            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * @author yellow013
     */
    private static class TimeClientHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf buf = (ByteBuf) msg; // (#)
            /*
             * #) 在 TCP/IP 中, Netty 会把读到的数据放入 ByteBuf 数据结构中.
             */
            try {
                long epochMillis = buf.readLong();
                System.out.println(LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(epochMillis),
                        ZoneId.systemDefault()));
                ctx.close();
            } finally {
                buf.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    public static void main(String[] args) throws Exception {
        new TimeClient("127.0.0.1", 8080).connect();
    }

}