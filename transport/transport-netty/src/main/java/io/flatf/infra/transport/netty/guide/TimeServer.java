package io.flatf.infra.transport.netty.guide;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class TimeServer {

    private final int port;

    private TimeServer(int port) {
        this.port = port;
    }

    public void startup() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(); // (*)
        /*
         * NioEventLoopGroup 是用来处理 I/O 操作的多线程事件循环器, Netty 提供了许多不同的 EventLoopGroup
         * 的实现用来处理不同的传输.
         *
         * 在这个例子中我们实现了一个服务端的应用, 因此会有2个 NioEventLoopGroup 会被使用. 第一个经常被叫做boss,
         * 用来接收进来的连接. 第二个经常被叫做worker, 用来处理已经被接收的连接, 一旦boss接收到连接,
         * 就会把连接信息注册到worker上.
         *
         * 如何知道多少个线程已经被使用, 如何映射到已经创建的 Channel上都需要依赖于 EventLoopGroup 的实现,
         * 并且可以通过构造函数来配置他们的关系.
         */
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap(); // (*)
            /*
             * ServerBootstrap 是一个启动 NIO 服务的辅助启动类.
             *
             * 可以在这个服务中直接使用 Channel, 但是这会是一个复杂的处理过程, 在很多情况下你并不需要这样做.
             */
            bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class) // (*)
                    /*
                     * 指定使用 NioServerSocketChannel 类来举例说明一个新的 Channel 如何接收进来的连接.
                     */
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (*)
                        /*
                         * 这里的事件处理类经常会被用来处理一个最近的已经接收的 Channel.
                         *
                         * ChannelInitializer 是一个特殊的处理类, 他的目的是帮助使用者配置一个新的 Channel.
                         *
                         * 也许你想通过增加一些处理类比如 EchoServerHandler 来配置一个新的 Channel 或者其对应的ChannelPipeline
                         * 来实现你的网络程序.
                         *
                         * 当你的程序变的复杂时，可能你会增加更多的处理类到 pipeline 上, 然后提取这些匿名类到最顶层的类上.
                         */
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new TimeServerHandler());
                        }
                    }).option(ChannelOption.SO_BACKLOG, 128) // (*)
                    /*
                     * 你可以设置这里指定的 Channel 实现的配置参数.
                     *
                     * 我们正在写一个 TCP/IP 的服务端, 因此我们被允许设置 socket 的参数选项比如 tcpNoDelay 和 keepAlive.
                     *
                     * 请参考 ChannelOption 和详细的 ChannelConfig 实现的接口文档以此可以对ChannelOption 的有一个大概的认识.
                     */
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (*)
            /*
             * option() 和 childOption().
             *
             * option() 是提供给 NioServerSocketChannel 用来接收进来的连接.
             *
             * childOption() 是提供给由父管道 ServerChannel 接收到的连接, 在这个例子中也是 NioServerSocketChannel.
             */
            ChannelFuture future = bootstrap.bind(port).sync(); // 绑定端口, 开始接收进来的连接 (*)
            /*
             * 绑定端口然后启动服务.
             *
             * 可以多次调用 bind() (基于不同绑定地址).
             */
            future.channel().closeFuture().sync(); // (*)
            /*
             * 等待服务器 socket 关闭.
             *
             * 在这个例子中, 这不会发生. 但你可以优雅地关闭你的服务器.
             */
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    /**
     * 在这个部分被实现的协议是 TIME 协议. <br>
     * <br>
     * 和之前的例子不同的是在不接受任何请求时他会发送一个含32位的整数的消息, 并且一旦消息发送就会立即关闭连接.<br>
     * <br>
     * 这个例子中, 你学习如何构建和发送一个消息, 然后在完成时关闭连接.<br>
     * <br>
     * 忽略任何接收到的数据，而只是在连接被创建发送一个消息，覆盖 channelActive() 方法.
     *
     * @author yellow013
     */
    private static class TimeServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(final ChannelHandlerContext ctx) { // (*)
            /*
             * channelActive() 方法将会在连接被建立并且准备进行通信时被调用.
             *
             * 因此让我们在这个方法里完成一个代表当前时间的32位整数消息的构建工作.
             */
            final ByteBuf buf = ctx.alloc().buffer(8); // (*)
            /*
             * 为了发送一个新的消息, 我们需要分配一个包含这个消息的新的缓冲.
             *
             * 因为我们需要写入一个32位的整数, 因此我们需要一个至少有4个字节的 ByteBuf.
             *
             * 通过 ChannelHandlerContext.alloc() 得到一个当前的ByteBufAllocator, 然后分配一个新的缓冲.
             */
            buf.writeLong(System.currentTimeMillis());

            final ChannelFuture cf = ctx.writeAndFlush(buf); // (*)
            /*
             * 和往常一样我们需要编写一个构建好的消息.
             *
             * ByteBuf 有两个指针, 一个对应读操作一个对应写操作.
             *
             * 当你向 ByteBuf 里写入数据的时候写指针的索引就会增加, 同时读指针的索引没有变化.
             *
             * 读指针索引和写指针索引分别代表了消息的开始和结束.
             *
             * ChannelHandlerContext.write() 和 writeAndFlush() 方法会返回一个 ChannelFuture 对象.
             *
             * 一个 ChannelFuture 代表了一个还没有发生的 I/O 操作.
             *
             * 这意味着任何一个请求操作都不会马上被执行, 因为在 Netty 里所有的操作都是异步的.
             */
            cf.addListener(future -> {
                assert cf == future;
                ctx.close();
            }); // (*)
            /*
             * 在返回的 ChannelFuture 上增加一个 ChannelFutureListener.
             *
             * 这里我们构建了一个匿名的 ChannelFutureListener 类用来在操作完成时关闭 Channel.
             */
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }

    }

    public static void main(String[] args) throws Exception {
        new TimeServer(8080).startup();
    }

}
