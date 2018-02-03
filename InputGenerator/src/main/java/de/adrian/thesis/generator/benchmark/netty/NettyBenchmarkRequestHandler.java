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

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles a server-side channel.
 */
@Sharable
public class NettyBenchmarkRequestHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger LOG = LogManager.getLogger(NettyBenchmarkRequestHandler.class);

    private final NettyForwardingThread forwardingThread;

    NettyBenchmarkRequestHandler(NettyForwardingThread forwardingThread) {
        this.forwardingThread = forwardingThread;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        NettyBenchmark.CHANNELS.add(ctx.channel());
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) throws Exception {

        LOG.info("Received '{}'", request);

        if (request.startsWith("start")) {
            forwardingThread.startConsuming(0);
        } else if (request.startsWith("from:")) {
            String[] split = request.split(":");
            long startingNumber = Long.parseLong(split[1]);
            forwardingThread.startConsuming(startingNumber);
        } else if (request.startsWith("stop")) {
            forwardingThread.stopConsuming();
            ctx.close();

//            Would close the whole application
//            ctx.channel().close();
//            ctx.channel().parent().close();
        } else {
            LOG.error("Received illegal command '{}'", request);
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        forwardingThread.stopConsuming();
        super.channelInactive(ctx);
        LOG.error("Channel {} became inactive", ctx.channel().remoteAddress());
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws InterruptedException {
        forwardingThread.stopConsuming();
        LOG.error("Error in channel: ", cause);
        ctx.close();
    }
}

