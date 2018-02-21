package de.adrian.thesis.generator.benchmark.netty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.Queue;

public class NettyThroughputLoggingThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(NettyThroughputLoggingThread.class);

    /**
     * Maximum capacity for {@link java.util.concurrent.LinkedBlockingQueue} - 1000
     */
    public static int MAX_QUEUE_SIZE;

    private static final Marker THROUGHPUT_MARKER = MarkerManager.getMarker("Throughput");
    private final String instanceName;
    private final Queue<String> queue;
    private final int queueID;

    private NettyStringForwardingThread forwardingThread;

    public NettyThroughputLoggingThread(Queue<String> queue,
                                        NettyStringForwardingThread forwardingThread,
                                        String instanceName,
                                        int queueId) {
        this.queue = queue;
        this.forwardingThread = forwardingThread;
        this.instanceName = instanceName;
        this.queueID = queueId;
    }

    public void setForwardingThread(NettyStringForwardingThread forwardingThread) {
        this.forwardingThread = forwardingThread;
    }

    @Override
    public void run() {

        long currentTime;
        int queueSize;

        while (true) {
            try {
                Thread.sleep(1000);

                // TODO Or use System.nanoTime()? Measure computational overhead
                currentTime = System.currentTimeMillis();

                queueSize = queue.size();

                LOG.info(THROUGHPUT_MARKER, "{},{},{},{},{},{}",
                        currentTime,
                        instanceName,
                        queueID,
                        forwardingThread.getForwardingId(),
                        queueSize,
                        forwardingThread.getCurrentRecords().get());
                forwardingThread.getCurrentRecords().set(0);

                if (queueSize >= MAX_QUEUE_SIZE) {
                    queue.clear();
                    LOG.error("Queue has reached its maximum capacity (QueueID {})", queueID);
                }

            } catch (InterruptedException e) {
                LOG.error("ThroughputLoggingThread was interrupted");
                return;
            }
        }
    }
}
