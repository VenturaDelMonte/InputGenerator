package de.adrian.thesis.generator.benchmark.netty;


import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import de.adrian.thesis.generator.benchmark.javaio.ThroughputLoggingThread;
import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NettyForwardingThread<T> extends Thread {

    private static final Logger LOG = LogManager.getLogger(NettyForwardingThread.class);

    private static final String THREAD_NAME = "NettyForwardingThread";

    private static final int WAITING_TIMEOUT = 10000;

    private final BlockingQueue<T> queue;
    private final NettyCreatorThread<T> producerThread;
    private final ThroughputLoggingThread loggingThread;
    private final AtomicLong currentRecords = new AtomicLong();
    private final SocketChannel channel;
    private final ForwardingThread.ForwardingThreadProperties forwardingProperties;

    private volatile boolean interrupted = false;
    private long sendRecords;

    NettyForwardingThread(SocketChannel channel,
                              RecordCreator<T> recordCreator,
                              CreatorThread.CreateThreadProperties creatorProperties,
                              ForwardingThread.ForwardingThreadProperties forwardingProperties,
                              String name) {
        super(THREAD_NAME);
        this.channel = channel;
        this.forwardingProperties = forwardingProperties;
        this.queue = new LinkedBlockingQueue<>();
        this.producerThread = new NettyCreatorThread<T>(channel, queue, recordCreator, creatorProperties);
        this.loggingThread = new ThroughputLoggingThread(currentRecords, name);
    }

    @Override
    public void run() {
        try {

            T record;

            while (!interrupted && channel.isActive()) {

                record = queue.poll(WAITING_TIMEOUT, TimeUnit.MILLISECONDS);

                if (record == null) {
                    LOG.error("NettyForwardingThread waited more than {} for new record. Shutting down", WAITING_TIMEOUT);
                    break;
                }

                channel.writeAndFlush(record);

                if (forwardingProperties.logMessages && sendRecords++ % forwardingProperties.logMessagesModulo == 0) {
                    LOG.info("NettyForwardingThread consumed '{}'", record);
                }

                sendRecords++;
                currentRecords.incrementAndGet();
            }
        } catch (InterruptedException exception) {
            LOG.error("NettyForwardingThread was interrupted");
        }

        try {
            close();
        } catch (InterruptedException e) {
            LOG.error("NettyForwardingThread was interrupted while finishing");
        }
    }

    void stopConsuming() throws InterruptedException {
        this.interrupt();
        interrupted = true;

        close();
    }

    private void close() throws InterruptedException {
        loggingThread.interrupt();
        producerThread.stopProducing();

        loggingThread.join();
        producerThread.join();

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

    public void startConsuming(long startNumber) {

        if (producerThread.isAlive()) {
            return;
        }

        producerThread.start(startNumber);
        loggingThread.start();
        this.start();
    }
}
