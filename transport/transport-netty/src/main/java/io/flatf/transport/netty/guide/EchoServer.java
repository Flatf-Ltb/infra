package io.flatf.transport.netty.guide;

import io.flatf.common.character.Charsets;
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

/**
 * 实现 ECHO 协议, 返回所有收到的数据. <br>
 * <br>
 * EchoServer 实现用于启动服务端的 EchoServer
 *
 * @author yellow013
 */
public class EchoServer {

    private final int port;

    private EchoServer(int port) {
        this.port = port;
    }

    public void startup() throws InterruptedException {
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
                            ch.pipeline().addLast(new EchoServerHandler());
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
     * 实现 ECHO 协议, 返回所有收到的数据. <br>
     * <br>
     * Handler(处理器) 实现，Handler 是由 Netty 生成用来处理 I/O 事件.
     *
     * @author yellow013
     */
    private static class EchoServerHandler extends ChannelInboundHandlerAdapter { // (*)
        /*
         * EchoServerHandler 继承自 ChannelInboundHandlerAdapter.
         *
         * 这个类实现了 ChannelInboundHandler 接口, ChannelInboundHandler 提供了许多事件处理的接口方法,
         * 然后你可以覆盖这些方法.
         *
         * 现在仅仅只需要继承 ChannelInboundHandlerAdapter 类而不是你自己去实现接口方法.
         */

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) { // (*)
            /*
             * 覆盖 chanelRead() 事件处理方法.
             *
             * 每当从客户端收到新的数据时, 这个方法会在收到消息时被调用, 这个例子中, 收到的消息的类型是 ByteBuf
             */
            ByteBuf buf = ((ByteBuf) msg);
            System.out.println(buf);
            System.out.println(buf.toString(Charsets.UTF8)); // 输出接收到的字符

            ctx.write(buf); // (*)
            /*
             * ChannelHandlerContext 对象提供了许多操作, 使你能够触发各种各样的 I/O 事件和操作.
             *
             * 这里我们调用了 write(Object) 方法来逐字地把接受到的消息写入.
             *
             * 请注意不同于 DISCARD 的例子我们并没有释放接受到的消息, 这是因为当写入的时候 Netty 已经帮我们释放了.
             */

            ctx.flush(); // (*)
            /*
             * ctx.write(Object) 方法不会使消息写入到通道上, 他被缓冲在了内部, 你需要调用 ctx.flush() 方法来把缓冲区中数据强行输出.
             *
             * 或者你可以用更简洁的 cxt.writeAndFlush(msg) 以达到同样的目的.
             */
        }

        /**
         * 连接激活事件
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("call channelActive");
            super.channelActive(ctx);
        }

        /**
         * 连接断开事件
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("call channelInactive");
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (*)
            /*
             * exceptionCaught() 事件处理方法是当出现 Throwable 对象才会被调用, 即当 Netty 由于 IO
             * 错误或者处理器在处理事件时抛出的异常时.
             *
             * 在大部分情况下, 捕获的异常应该被记录下来并且把关联的 channel 给关闭掉.
             *
             * 然而这个方法的处理方式会在遇到不同异常的情况下有不同的实现, 比如你可能想在关闭连接之前发送一个错误码的响应消息.
             */
            cause.printStackTrace(); // 当出现异常就关闭连接
            ctx.close();
        }
    }

    public static void main(String[] args) throws Exception {
        new EchoServer(8080).startup();
    }

}
