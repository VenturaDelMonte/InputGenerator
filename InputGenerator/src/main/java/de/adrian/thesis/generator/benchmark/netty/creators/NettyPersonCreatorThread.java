package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.nexmark.NexmarkStreamGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;

/**
 * Generates person instances as csv for the Nexmark benchmark.
 */
public class NettyPersonCreatorThread extends AbstractNettyCreatorThread {

    private final static String THREAD_NAME = "NettyPersonCreatorThread";

    public static long WAIT_DURATION = 100;

    public NettyPersonCreatorThread(Queue<String> queue, AbstractNettyCreatorThreadProperties properties) {
        super(THREAD_NAME, queue, properties);
    }

    @Override
    String generateRecord() {
        return nexmarkGenerator.generatePerson();
    }

    @Override
    long getWaitingDuration() {
        return WAIT_DURATION;
    }

    @Override
    public String getShortDescription() {
        return "persons";
    }
}
