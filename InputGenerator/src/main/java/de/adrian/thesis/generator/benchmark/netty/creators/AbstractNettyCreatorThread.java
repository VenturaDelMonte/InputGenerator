package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.nexmark.NexmarkStreamGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;

/**
 * Abstract creator for the NexmarkBenchmark
 */
public abstract class AbstractNettyCreatorThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(AbstractNettyCreatorThread.class);

    final NexmarkStreamGenerator nexmarkGenerator;
    private final Queue<String> queue;
    private final AbstractNettyCreatorThreadProperties properties;

    private volatile boolean interrupted = false;

    AbstractNettyCreatorThread(String threadName, Queue<String> queue, AbstractNettyCreatorThreadProperties properties) {
        super(threadName);
        this.queue = queue;
        this.properties = properties;
        this.nexmarkGenerator = NexmarkStreamGenerator.GetInstance();
    }

    @Override
    public void run() {

        // Generate initial numbers
        long counter = 0;
        for (; counter < properties.initialRecords && !interrupted; counter++) {

            String record = generateRecord();

            queue.add(record);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Initially inserted '{}' into queue", record);
            }
        }

        for (; counter < properties.maxNumbers && !interrupted; counter++) {

            String record = generateRecord();

            queue.add(record);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Inserted '{}' into queue", record);
            }

            try {
                Thread.sleep(getWaitingDuration());
            } catch (InterruptedException e) {
                LOG.error("NettyCreatorThread was interrupted: {}", e.getLocalizedMessage());
                break;
            }
        }
    }

    abstract String generateRecord();

    abstract long getWaitingDuration();

    public abstract String getShortDescription();

    public void stopProducing() {
        interrupt();
        interrupted = true;
    }

    public static class AbstractNettyCreatorThreadProperties extends CreatorThread.CreateThreadProperties {

        int initialRecords;

        public AbstractNettyCreatorThreadProperties(CreatorThread.CreateThreadProperties properties) {
            this.delay = properties.delay;
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
