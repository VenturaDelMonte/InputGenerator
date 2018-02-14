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

import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import de.adrian.thesis.generator.benchmark.netty.creators.*;
import de.adrian.thesis.generator.benchmark.netty.creators.recordcreator.RecordCreator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles a server-side channel.
 */
@Sharable
public class NettyBenchmarkRequestHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger LOG = LogManager.getLogger(NettyBenchmarkRequestHandler.class);

    private static final Map<Integer, CreatorLoggingPair> CREATORS = new ConcurrentHashMap<>();

    private final RecordCreator recordCreator;
    private final AbstractNettyCreatorThread.AbstractNettyCreatorThreadProperties creatorProperties;
    private final ForwardingThread.ForwardingThreadProperties forwardingProperties;
    private final String instanceName;
    private final int instanceNumber;

    private NettyStringForwardingThread forwardingThread;

    NettyBenchmarkRequestHandler(RecordCreator recordCreator,
                                 AbstractNettyCreatorThread.AbstractNettyCreatorThreadProperties creatorProperties,
                                 ForwardingThread.ForwardingThreadProperties forwardingProperties,
                                 int instanceNumber) {
        this.recordCreator = recordCreator;
        this.creatorProperties = creatorProperties;
        this.forwardingProperties = forwardingProperties;
        this.instanceName = forwardingProperties.name;
        this.instanceNumber = instanceNumber;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        NettyBenchmark.CHANNELS.add(ctx.channel());
    }

    /**
     * TODO Check for correct state when receiving contradicting requests
     * Each requests expects the following format: {@code FlinkSourceNumber:Command:AdditionalParameters}
     *
     * @param ctx     ChannelHandler Context
     * @param request String request from the client
     * @throws Exception thrown in case of errors
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) throws Exception {

        LOG.info("Received '{}'", request);

        String[] split = request.split(":");

        if (split.length < 2) {
            throw new IllegalStateException("Did not receive correct command from client");
        }

        int sourceID = Integer.parseInt(split[0]);
        AbstractNettyCreatorThread creatorThread;

        switch (split[1]) {
            case "start":
                creatorThread = new NettyStringCreatorThread(recordCreator, creatorProperties, 0);
                break;
            case "from":
                long startingNumber = Long.parseLong(split[2]);
                creatorThread = new NettyStringCreatorThread(recordCreator, creatorProperties, startingNumber);
                break;
            case "stop":
                forwardingThread.stopConsuming();
                ctx.close();

                //            Would close the whole application
                //            ctx.channel().close();
                //            ctx.channel().parent().close();
                return;
            case "persons":
                creatorThread = new NettyPersonCreatorThread(creatorProperties);
                break;
            case "auctions":
                creatorThread = new NettyAuctionCreatorThread(creatorProperties);
                break;
            case "yahoo":
                creatorThread = new NettyYahooCreatorThread(creatorProperties);
                break;
            default:
                LOG.error("Received illegal command '{}'", request);
                ctx.close();
                return;
        }

        createProducerOrConnectToExisting(creatorThread, sourceID, ctx.channel());
    }

    private void createProducerOrConnectToExisting(AbstractNettyCreatorThread creatorThread, int sourceID, Channel channel) {

        CreatorLoggingPair pair = CREATORS.get(sourceID);

        if (pair != null) {

            forwardingThread = new NettyStringForwardingThread(channel, pair.creatorThread.getQueue(), instanceNumber, forwardingProperties);

            pair.loggingThread.setForwardingThread(forwardingThread);

            forwardingThread.start();

        } else {
            forwardingThread = new NettyStringForwardingThread(channel, creatorThread.getQueue(), instanceNumber, forwardingProperties);

            NettyThroughputLoggingThread loggingThread = new NettyThroughputLoggingThread(creatorThread.getQueue(), forwardingThread, instanceName);

            creatorThread.start();
            loggingThread.start();
            forwardingThread.start();

            CREATORS.put(sourceID, new CreatorLoggingPair(creatorThread, loggingThread));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        stopForwardingThread();
        LOG.error("Channel {} became inactive", ctx.channel().remoteAddress());
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Error in channel: ", cause);
        stopForwardingThread();
        ctx.close();
    }

    private void stopForwardingThread() {
        if (forwardingThread != null) {
            forwardingThread.stopConsuming();
        }
    }

    public class CreatorLoggingPair {
        final AbstractNettyCreatorThread creatorThread;
        final NettyThroughputLoggingThread loggingThread;

        CreatorLoggingPair(AbstractNettyCreatorThread creatorThread, NettyThroughputLoggingThread loggingThread) {
            this.creatorThread = creatorThread;
            this.loggingThread = loggingThread;
        }

        @Override
        public String toString() {
            return "(" + creatorThread + "," + loggingThread + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }

            if (!(other instanceof CreatorLoggingPair)) {
                return false;
            }

            CreatorLoggingPair other_ = (CreatorLoggingPair) other;

            return other_.creatorThread.equals(this.creatorThread) && other_.loggingThread.equals(this.loggingThread);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((creatorThread == null) ? 0 : creatorThread.hashCode());
            result = prime * result + ((loggingThread == null) ? 0 : loggingThread.hashCode());
            return result;
        }
    }
}

