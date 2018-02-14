package de.adrian.thesis.generator.benchmark.netty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class NettyThroughputLoggingThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(NettyThroughputLoggingThread.class);

    private static final Marker THROUGHPUT_MARKER = MarkerManager.getMarker("Throughput");
    private final String instanceName;
    private final Queue<String> queue;
    private final int forwardingIdCreator;

    private NettyStringForwardingThread forwardingThread;

    public NettyThroughputLoggingThread(Queue<String> queue, NettyStringForwardingThread forwardingThread, String instanceName) {
        this.queue = queue;
        this.forwardingThread = forwardingThread;
        this.instanceName = instanceName;
        this.forwardingIdCreator = forwardingThread.getForwardingId();
    }

    public void setForwardingThread(NettyStringForwardingThread forwardingThread) {
        this.forwardingThread = forwardingThread;
    }

    @Override
    public void run() {

        long currentTime;

        while (true) {
            try {
                Thread.sleep(1000);

                // TODO Or use System.nanoTime()? Measure computational overhead
                currentTime = System.currentTimeMillis();

                LOG.info(THROUGHPUT_MARKER, "{},{},{},{},{},{}",
                        currentTime,
                        instanceName,
                        forwardingIdCreator,
                        forwardingThread.getForwardingId(),
                        queue.size(),
                        forwardingThread.getCurrentRecords().get());
                forwardingThread.getCurrentRecords().set(0);

            } catch (InterruptedException e) {
                LOG.error("ThroughputLoggingThread was interrupted");
                return;
            }
        }
    }
}
