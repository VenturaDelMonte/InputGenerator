package de.adrian.thesis.generator.benchmark.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.timeout.IdleStateHandler;

public class NettyBenchmarkServerInitializer extends ChannelInitializer<Channel> {

    private final int serverTimeout;

    NettyBenchmarkServerInitializer(int serverTimeout) {
        this.serverTimeout = serverTimeout;
    }

    @Override
    public void initChannel(Channel channel) {
        channel.pipeline().addLast("idleHandler", new IdleStateHandler(
                serverTimeout,
                serverTimeout,
                serverTimeout));
        channel.pipeline().addLast("channelCloser", new NettyBenchmarkServerHandler(serverTimeout));
    }
}