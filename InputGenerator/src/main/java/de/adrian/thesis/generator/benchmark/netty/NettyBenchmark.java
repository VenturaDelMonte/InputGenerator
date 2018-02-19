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

import com.beust.jcommander.Parameter;
import de.adrian.thesis.generator.benchmark.Benchmark;
import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import de.adrian.thesis.generator.benchmark.netty.creators.*;
import de.adrian.thesis.generator.yahoo.YahooMatchingUUIDGenerator;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Sends records based on the native transport layer Epoll and uses asynchronous IO using Netty.
 */
public final class NettyBenchmark extends Benchmark {

    private static final Logger LOG = LogManager.getLogger(NettyBenchmark.class);

    static final ChannelGroup CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    @Parameter(names = {"-iP", "--initialRecords"}, description = "Set initial people to create")
    private int initialPersons = 50;

    @Parameter(names = {"-seed", "--yahooSeed"}, description = "Sets seed for the yahoo generator")
    private int yahooSeed = 1337;

    @Parameter(names = {"-cN", "--campaignNumber"}, description = "Sets number of campaigns for the yahoo generator")
    private int numberOfCampaigns = 100;

    @Parameter(names = {"-yN", "--yahooGeneratorName"}, description = "Sets the generator for the yahoo benchmark")
    private String yahooGeneratorName = "notIndependent";

    @Parameter(names = {"-mQ", "--maxQueueSize"}, description = "Sets the max queue size to prevent overflows")
    private int maxQueueSize = 50_000_000;

    @Parameter(names = {"-cT", "--cropTimestamp"}, description = "Crops the timestamps for the Yahoo benchmark to 1 second")
    private boolean cropTimestamp = false;

    private NettyBenchmark(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        NettyBenchmark nettyBenchmark = new NettyBenchmark(args);
        nettyBenchmark.parseCLI(args);
        nettyBenchmark.startGenerator();
    }

    @Override
    public void startGenerator() {

        LOG.info("Starting NettyBenchmark with maxMessages: {}, initialPersons: {}, " +
                        "messagePerSecond: {}, yahooGeneratorName: {}, port: {}, name: {}, timeout: {}",
                maxNumberOfMessages, initialPersons, messagesPerSecond, yahooGeneratorName, port, name, serverTimeout);

        NettyYahooCreatorThread.INITIAL_SEED = yahooSeed;
        NettyYahooCreatorThread.GENERATOR_NAME = yahooGeneratorName;
        NettyYahooCreatorThread.NUMBER_OF_CAMPAIGNS = numberOfCampaigns;
        NettyThroughputLoggingThread.MAX_QUEUE_SIZE = maxQueueSize;
        YahooMatchingUUIDGenerator.CROP_TIMESTAMPS = cropTimestamp;

        AbstractNettyCreatorThread.AbstractNettyCreatorThreadProperties creatorProperties =
                new AbstractNettyCreatorThread.AbstractNettyCreatorThreadProperties(getCreatorProperties());
        creatorProperties.setInitialRecords(initialPersons);

        ForwardingThread.ForwardingThreadProperties forwardingProperties = getForwardingProperties();

        boolean useEpoll = Epoll.isAvailable();

        EventLoopGroup bossGroup = useEpoll ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = useEpoll ? new EpollEventLoopGroup(4) : new NioEventLoopGroup(4);

        try {

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workerGroup)
                    .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
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

