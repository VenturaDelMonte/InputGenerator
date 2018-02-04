package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.benchmark.netty.creators.AbstractNettyCreatorThread;

import java.util.Queue;

/**
 * Generates person instances as csv for the Nexmark benchmark.
 */
public class NettyAuctionCreatorThread extends AbstractNettyCreatorThread {

    private final static String THREAD_NAME = "NettyAuctionCreatorThread";

    public static long WAIT_DURATION = 100;

    public NettyAuctionCreatorThread(Queue<String> queue, AbstractNettyCreatorThreadProperties properties) {
        super(THREAD_NAME, queue, properties);
    }

    @Override
    String generateRecord() {
        return nexmarkGenerator.generateAuction();
    }

    @Override
    long getWaitingDuration() {
        return WAIT_DURATION;
    }
}
