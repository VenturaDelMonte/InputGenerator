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

import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import de.adrian.thesis.generator.benchmark.recordcreator.CountingRecordCreator;
import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 */
public class NettyBenchmarkRequestInitializer<T> extends ChannelInitializer<SocketChannel> {

    private static final Logger LOG = LogManager.getLogger(NettyBenchmarkRequestInitializer.class);

    private static final StringDecoder DECODER = new StringDecoder();
    private static final StringEncoder ENCODER = new StringEncoder();

    private static final int MAX_FRAME_LENGTH = 8092;

    private final RecordCreator<T> recordCreator;
    private final CreatorThread.CreateThreadProperties creatorProperties;
    private final ForwardingThread.ForwardingThreadProperties forwardingProperties;

    private int instanceNumber = 0;

    NettyBenchmarkRequestInitializer(RecordCreator<T> recordCreator, CreatorThread.CreateThreadProperties creatorProperties,
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

        ByteBuf[] delimiters = new ByteBuf[] {
                Unpooled.wrappedBuffer(new byte[] { '\r', '\n' }), // WINDOWS
                Unpooled.wrappedBuffer(new byte[] { '\n' }),       // UNIX / OSX
                Unpooled.wrappedBuffer(new byte[] { '\r' })        // LEGACY MAC
        };
        pipeline.addLast("DelimiterFramer", new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, delimiters));

        pipeline.addLast(DECODER);
        pipeline.addLast(ENCODER);

        NettyForwardingThread<String> forwardingThread =
                new NettyForwardingThread(channel,
                        recordCreator,
                        creatorProperties,
                        forwardingProperties,
                        forwardingProperties.name + instanceNumber++);

        pipeline.addLast(new NettyBenchmarkRequestHandler(forwardingThread));
    }
}

