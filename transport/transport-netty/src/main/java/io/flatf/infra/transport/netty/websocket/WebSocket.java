package io.flatf.infra.transport.netty.websocket;

import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

public class WebSocket {

    public static void main(String[] args) {
        final Logger log = Log4j2LoggerFactory.getLogger(WebSocket.class);
        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            URI uri = new URI("ws://192.168.50.xx:xx/xx/xx");
            Bootstrap bootstrap = new Bootstrap();
            MockClientHandler webSocketClientHandler = new MockClientHandler(WebSocketClientHandshakerFactory
                    .newHandshaker(uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders()));
            bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class)
                    .handler(new MockClientInitializer(webSocketClientHandler));
            Channel channel = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException | URISyntaxException e) {
            log.error("socket连接异常:{}", e.getMessage(), e);
        } finally {
            eventLoopGroup.shutdownGracefully();
        }

    }

    /**
     * @author yellow013
     */
    public static class MockClientInitializer extends ChannelInitializer<SocketChannel> {

        private final MockClientHandler mockClientHandler;

        private MockClientInitializer(MockClientHandler mockClientHandler) {
            this.mockClientHandler = mockClientHandler;
        }

        @Override
        protected void initChannel(SocketChannel channel) {
            channel.pipeline()
                    // 将请求与应答消息编码或者解码为HTTP消息
                    .addLast(new HttpClientCodec()).addLast(new LoggingHandler(LogLevel.INFO))
                    .addLast("decoder", new StringDecoder()).addLast("encoder", new StringEncoder())
                    // 客户端Handler
                    .addLast("handler", mockClientHandler);
        }
    }

    /**
     * @author yellow013
     */
    public static class MockClientHandler extends SimpleChannelInboundHandler<String> {

        private final Logger log = Log4j2LoggerFactory.getLogger(MockClientHandler.class);

        private final WebSocketClientHandshaker handshaker;

        private MockClientHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        /**
         * 当客户端主动链接服务端的链接后，调用此方法
         *
         * @param context ChannelHandlerContext
         */
        @Override
        public void channelActive(ChannelHandlerContext context) {
            log.info("""
                    \t⌜⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓
                    \t├ [Mock 建立连接]
                    \t⌞⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓""");
            final Channel channel = context.channel();
            // 握手
            handshaker.handshake(channel);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, String data) {
            log.info("接收到客户端的响应为:{}", data);
            // 自定义处理消息
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.info("""
                            \t⌜⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓
                            \t├ [exception]: {}
                            \t⌞⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓⎓""",
                    cause.getMessage());
            ctx.close();
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            System.out.println("与服务器端断开连接");
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext channelHandlerContext) {
            channelHandlerContext.flush();
        }

    }
}
