package de.adrian.thesis.generator.benchmark.netty;


import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NettyStringForwardingThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(NettyStringForwardingThread.class);

    private static final String THREAD_NAME = "NettyForwardingThread";

    private static final int WAITING_TIMEOUT = 10000;

    private final AtomicLong currentRecords = new AtomicLong();
    private final Channel channel;
    private final BlockingQueue<String> queue;
    private final int forwardingId;
    private final ForwardingThread.ForwardingThreadProperties forwardingProperties;

    private volatile boolean interrupted = false;
    private long sentRecords;

    NettyStringForwardingThread(Channel channel, BlockingQueue<String> queue, int sourceID, ForwardingThread.ForwardingThreadProperties forwardingProperties) {
        super(THREAD_NAME);
        this.channel = channel;
        this.queue = queue;
        this.forwardingId = sourceID;
        this.forwardingProperties = forwardingProperties;
    }

    @Override
    public void run() {
        try {

            String record;

            while (!interrupted && channel.isActive()) {

                record = queue.poll(WAITING_TIMEOUT, TimeUnit.MILLISECONDS);

                if (record == null) {
                    LOG.error("NettyForwardingThread waited more than {} for new record. Shutting down", WAITING_TIMEOUT);
                    break;
                }

                channel.writeAndFlush(record);

                if (forwardingProperties.logMessages && sentRecords++ % forwardingProperties.logMessagesModulo == 0) {
                    LOG.info("NettyForwardingThread consumed '{}'", record);
                }

                sentRecords++;
                currentRecords.incrementAndGet();
            }
        } catch (InterruptedException exception) {
            LOG.error("NettyForwardingThread was interrupted");
        }

        close();
    }

    void stopConsuming() {
        this.interrupt();
        interrupted = true;

        close();
    }

    private void close() {
        if (!channel.close().isDone()) {
            channel.close().addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    LOG.error("Successfully closed channel {}", channel.remoteAddress());
                } else {
                    LOG.error("Failed to close channel {}", channel.remoteAddress());
                }
            });
        }
    }

    public AtomicLong getCurrentRecords() {
        return currentRecords;
    }

    public int getForwardingId() {
        return forwardingId;
    }
}
