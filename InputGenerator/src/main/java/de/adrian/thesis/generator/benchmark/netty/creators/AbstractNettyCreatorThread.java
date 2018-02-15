package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Abstract creator for the NexmarkBenchmark
 */
public abstract class AbstractNettyCreatorThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(AbstractNettyCreatorThread.class);

    private final BlockingQueue<String> queue;
    private final AbstractNettyCreatorThreadProperties properties;

    private volatile boolean interrupted = false;
    long counter = 0;

    AbstractNettyCreatorThread(String threadName, AbstractNettyCreatorThreadProperties properties) {
        super(threadName);
        this.queue = new LinkedBlockingQueue<>();
        this.properties = properties;
    }

    @Override
    public void run() {

        // Generate initial numbers
        for (; counter < properties.initialRecords && !interrupted; counter++) {

            String record = generateRecord(counter);

            queue.add(record);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Initially inserted '{}' into queue", record);
            }
        }

        while (counter < properties.maxNumbers && !interrupted) {
            long emitStartTime = System.currentTimeMillis();

            int i = 0;
            for (; i < properties.messagesPerSecond && counter + i < properties.maxNumbers; i++) {
                String record = generateRecord(counter);

                queue.add(record);
            }

            counter += i;

            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                try {
                    Thread.sleep(1000 - emitTime);
                } catch (InterruptedException e) {
                    LOG.error("NettyCreatorThread was interrupted: {}", e.getLocalizedMessage());
                    break;
                }
            }
        }
    }

    abstract String generateRecord(long currentNumber);

    public abstract String getShortDescription();

    public void stopProducing() {
        interrupt();
        interrupted = true;
    }

    public BlockingQueue<String> getQueue() {
        return queue;
    }

    public static class AbstractNettyCreatorThreadProperties extends CreatorThread.CreateThreadProperties {

        int initialRecords;

        public AbstractNettyCreatorThreadProperties(CreatorThread.CreateThreadProperties properties) {
            this.messagesPerSecond = properties.messagesPerSecond;
            this.logMessages = properties.logMessages;
            this.logMessagesModulo = properties.logMessagesModulo;
            this.maxNumbers = properties.maxNumbers;
        }

        public AbstractNettyCreatorThreadProperties setInitialRecords(int initialRecords) {
            this.initialRecords = initialRecords;
            return this;
        }
    }
}
