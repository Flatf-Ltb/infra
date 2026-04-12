package io.flatf.transport.netty;

import io.flatf.common.log4j2.Log4j2LoggerFactory;
import io.flatf.common.sequence.SysNanoSequence;
import io.flatf.transport.TransportComponent;
import io.flatf.transport.api.Sender;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

public class NettySender extends TransportComponent implements Sender<byte[]> {

    private static final Logger log = Log4j2LoggerFactory.getLogger(NettySender.class);

    private final ChannelHandlerContext context;

    // private ByteBuf byteBuf;

    public NettySender(ChannelHandlerContext context) {
        this.context = context;
        // this.byteBuf = byteBuf;
    }

    // public NettySender(ChannelHandlerContext context) {
    // this(context);
    // }

    @Override
    public boolean isConnected() {
        context.disconnect();
        return true;
    }

    @Override
    public boolean closeIgnoreException() {
        // byteBuf.release();
        context.disconnect();
        context.close();
        newEndTime();
        return true;
    }

    @Override
    public String getName() {
        return "NettySender-ContextHashCode:" + context.hashCode() + "&";// + byteBuf.capacity();
    }

    @Override
    public void send(byte[] msg) {
        log.debug("{} call sender send -> data length : {}", SysNanoSequence.getMicros(), msg.length);
        ByteBuf byteBuf = context.alloc().buffer(msg.length);
        byteBuf.writeBytes(msg);
        ChannelFuture writeAndFlush = context.writeAndFlush(byteBuf.retain());
        writeAndFlush.addListener(future -> {
            log.debug("{} call sender send operation complete -> data length : {}", SysNanoSequence.getMicros(),
                    byteBuf.writerIndex());
            byteBuf.clear();
            byteBuf.release();
        });
    }

}