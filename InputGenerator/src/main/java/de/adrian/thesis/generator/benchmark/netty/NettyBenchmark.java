package de.adrian.thesis.generator.benchmark.netty;

/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import de.adrian.thesis.generator.benchmark.Benchmark;
import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Sends records based on the native transport layer Epoll and uses asynchronous IO using Netty.
 */
public final class NettyBenchmark extends Benchmark {

    private static final Logger LOG = LogManager.getLogger(NettyBenchmark.class);

    static final ChannelGroup CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private NettyBenchmark(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        NettyBenchmark nettyBenchmark = new NettyBenchmark(args);
        nettyBenchmark.startGenerator();
    }

    @Override
    public NettyBenchmark getInstance() {
        return this;
    }

    @Override
    public void startGenerator() {

        CreatorThread.CreateThreadProperties creatorProperties = getCreatorProperties();
        ForwardingThread.ForwardingThreadProperties forwardingProperties = getForwardingProperties();

        EventLoopGroup bossGroup = new EpollEventLoopGroup(1);
        EventLoopGroup workerGroup = new EpollEventLoopGroup(4);

        try {

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workerGroup)
                    .channel(EpollServerSocketChannel.class)
                    .handler(new NettyBenchmarkServerInitializer(forwardingProperties.serverTimeout))
                    .childHandler(new NettyBenchmarkRequestInitializer(recordCreator, creatorProperties, forwardingProperties));

            server.bind(port).sync().channel().closeFuture().sync();
        } catch (InterruptedException exception) {
            LOG.error("NettyBenchmark was interrupted", exception);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

        // Even though we arrive here, java program does not close. Strange issue.
        // Therefore, we need to close the program this way
        System.exit(0);
    }
}

