package de.adrian.thesis.generator.benchmark.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NettyBenchmarkServerHandler extends ChannelDuplexHandler {

    private static final Logger LOG = LogManager.getLogger(NettyBenchmarkServerHandler.class);

    private final int timeout;

    NettyBenchmarkServerHandler(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                if (NettyBenchmark.CHANNELS.isEmpty()) {
                    LOG.info("Closing server after no request for {} seconds", timeout);
                    ctx.close();
                }
            }
        }

    }
}
