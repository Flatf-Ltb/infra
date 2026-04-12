package io.flatf.transport.netty.handler;

import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;

public abstract class GeneralNettyHandler extends ChannelInboundHandlerAdapter {

    // buffer size
    protected int byteBufSize;

    private static final Logger log = Log4j2LoggerFactory.getLogger(GeneralNettyHandler.class);

    /**
     * Use default bytebufSize;
     * <p>
     * Size = 1024 * 8.
     */
    public GeneralNettyHandler() {
        this(1024 * 8);
    }

    /**
     * @param byteBufSize int
     */
    public GeneralNettyHandler(int byteBufSize) {
        this.byteBufSize = byteBufSize;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        ctx.close();
    }

    protected byte[] getRecvBytes(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.retain().readBytes(bytes);
        buf.release();
        return bytes;
    }

    protected ByteBuf allocByteBuf(ChannelHandlerContext ctx) {
        return allocByteBuf(ctx, this.byteBufSize);
    }

    protected ByteBuf allocByteBuf(ChannelHandlerContext ctx, int byteBufSize) {
        return ctx.alloc().buffer(byteBufSize);
    }

    protected void sendBytes(ChannelHandlerContext ctx, byte[] bytes) {
        ByteBuf byteBuf = ctx.alloc().buffer(bytes.length);
        byteBuf.writeBytes(bytes);
        ctx.writeAndFlush(byteBuf.retain()).addListener(future -> {
            byteBuf.clear();
            byteBuf.release();
        });
    }

}
