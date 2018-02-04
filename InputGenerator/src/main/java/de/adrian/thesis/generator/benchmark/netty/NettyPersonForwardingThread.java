package de.adrian.thesis.generator.benchmark.netty;


import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import de.adrian.thesis.generator.benchmark.javaio.ThroughputLoggingThread;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NettyPersonForwardingThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(NettyPersonForwardingThread.class);

    private static final String THREAD_NAME = "NettyPersonForwardingThread";

    private static final int WAITING_TIMEOUT = 100;

    private final BlockingQueue<String> queue;
    private final NettyPersonCreatorThread producerThread;
    private final ThroughputLoggingThread loggingThread;
    private final AtomicLong currentRecords = new AtomicLong();
    private final SocketChannel channel;
    private final ForwardingThread.ForwardingThreadProperties forwardingProperties;

    private volatile boolean interrupted = false;
    private long sendRecords;

    NettyPersonForwardingThread(SocketChannel channel,
                                NettyPersonCreatorThread.NettyPersonCreatorThreadProperties creatorProperties,
                                ForwardingThread.ForwardingThreadProperties forwardingProperties,
                                String name) {
        super(THREAD_NAME);
        this.channel = channel;
        this.forwardingProperties = forwardingProperties;
        this.queue = new LinkedBlockingQueue<>();
        this.producerThread = new NettyPersonCreatorThread(queue, creatorProperties);
        this.loggingThread = new ThroughputLoggingThread(currentRecords, name);
    }

    @Override
    public void run() {
        try {

            while (!interrupted && channel.isActive()) {

                String person = queue.poll(WAITING_TIMEOUT, TimeUnit.MILLISECONDS);

                if (person == null) {
                    break;
                }

                channel.writeAndFlush(person);

                if (forwardingProperties.logMessages && sendRecords++ % forwardingProperties.logMessagesModulo == 0) {
                    LOG.info("NettyForwardingThread consumed '{}'", person);
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

    public void startConsuming() {

        if (producerThread.isAlive()) {
            return;
        }

        producerThread.start();
        loggingThread.start();
        this.start();
    }
}
