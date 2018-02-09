package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.benchmark.netty.creators.AbstractNettyCreatorThread;
import de.adrian.thesis.generator.nexmark.NexmarkStreamGenerator;

import java.util.Queue;

/**
 * Generates person instances as csv for the Nexmark benchmark.
 */
public class NettyAuctionCreatorThread extends AbstractNettyCreatorThread {

    private final static String THREAD_NAME = "NettyAuctionCreatorThread";

    public static long WAIT_DURATION = 10;

    private final NexmarkStreamGenerator nexmarkGenerator;

    public NettyAuctionCreatorThread(Queue<String> queue, AbstractNettyCreatorThreadProperties properties) {
        super(THREAD_NAME, queue, properties);
        this.nexmarkGenerator = new NexmarkStreamGenerator();
    }

    @Override
    String generateRecord() {
        return nexmarkGenerator.generateAuction();
    }

    @Override
    long getWaitingDuration() {
        return WAIT_DURATION;
    }

    @Override
    public String getShortDescription() {
        return "auctions";
    }
}
