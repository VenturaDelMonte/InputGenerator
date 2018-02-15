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
import de.adrian.thesis.generator.benchmark.netty.creators.AbstractNettyCreatorThread;
import de.adrian.thesis.generator.benchmark.netty.creators.recordcreator.RecordCreator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.LineEncoder;
import io.netty.handler.codec.string.LineSeparator;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 */
public class NettyBenchmarkRequestInitializer<T> extends ChannelInitializer<SocketChannel> {

    private static final Logger LOG = LogManager.getLogger(NettyBenchmarkRequestInitializer.class);

    private static final StringDecoder DECODER = new StringDecoder();

    private static final int MAX_FRAME_LENGTH = 8092;

    private final RecordCreator recordCreator;
    private final AbstractNettyCreatorThread.AbstractNettyCreatorThreadProperties creatorProperties;
    private final ForwardingThread.ForwardingThreadProperties forwardingProperties;

    private AtomicInteger instanceNumber = new AtomicInteger();

    NettyBenchmarkRequestInitializer(RecordCreator recordCreator,
                                     AbstractNettyCreatorThread.AbstractNettyCreatorThreadProperties creatorProperties,
                                     ForwardingThread.ForwardingThreadProperties forwardingProperties) {
        this.recordCreator = recordCreator;
        this.creatorProperties = creatorProperties;
        this.forwardingProperties = forwardingProperties;
    }

    @Override
    public void initChannel(SocketChannel channel) {

        LOG.info("Client connected from {}:{}",
                channel.remoteAddress().getHostName(), channel.remoteAddress().getPort());

        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast("DelimiterFramer", new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, Delimiters.lineDelimiter()));
        pipeline.addLast(DECODER);

        pipeline.addLast("LineEncoder", new LineEncoder(LineSeparator.DEFAULT, CharsetUtil.UTF_8));

        pipeline.addLast(new NettyBenchmarkRequestHandler(recordCreator, creatorProperties, forwardingProperties, instanceNumber.getAndIncrement()));
    }
}

