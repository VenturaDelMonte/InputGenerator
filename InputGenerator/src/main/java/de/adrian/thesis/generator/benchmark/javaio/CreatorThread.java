package de.adrian.thesis.generator.benchmark.javaio;

import de.adrian.thesis.generator.benchmark.netty.creators.recordcreator.RecordCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;

public class CreatorThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(CreatorThread.class);

    private final static String THREAD_NAME = "CreatorThread";

    private final Queue<String> queue;
    private final RecordCreator recordCreator;
    private final CreateThreadProperties properties;
    private final SocketBenchmarkCallback finisher;
    private long startingNumber = 0;
    private volatile boolean interrupted = false;

    CreatorThread(SocketBenchmarkCallback finisher,
                  Queue<String> queue,
                  RecordCreator recordCreator,
                  CreateThreadProperties properties) {
        super(THREAD_NAME);
        this.finisher = finisher;
        this.queue = queue;
        this.recordCreator = recordCreator;
        this.properties = properties;
    }

    public synchronized void start(long startingNumber) {
        this.startingNumber = startingNumber;
        super.start();
    }

    @Override
    public void run() {

        if (startingNumber >= properties.maxNumbers) {
            finisher.finishApplication("Starting number already bigger that maxNumber");
        }

        for (long counter = startingNumber; counter < properties.maxNumbers && !interrupted; counter++) {
            String record = recordCreator.createRecord(counter);
            queue.add(record);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Inserted '{}' into queue", record);
            }

            try {
                Thread.sleep(properties.messagesPerSecond);
                throw new IllegalStateException("Currently not functional, look at NettyCreatorThread");
            } catch (InterruptedException e) {
                LOG.error("CreatorThread was interrupted: {}", e.getLocalizedMessage());
                break;
            }
        }
    }

    void stopProducing() {
        interrupt();
        interrupted = true;
    }

    public static class CreateThreadProperties {
        public int messagesPerSecond = 50;
        public long maxNumbers = -1;
        public boolean logMessages = true;
        public int logMessagesModulo = 50;

        public CreateThreadProperties setMessagesPerSecond(int messagesPerSecond) {
            this.messagesPerSecond = messagesPerSecond;
            return this;
        }

        public CreateThreadProperties setMaxNumbers(long maxNumbers) {
            this.maxNumbers = maxNumbers;
            return this;
        }

        public CreateThreadProperties setLogMessages(boolean logMessages) {
            this.logMessages = logMessages;
            return this;
        }

        public CreateThreadProperties setLogMessagesModulo(int logMessagesModulo) {
            this.logMessagesModulo = logMessagesModulo;
            return this;
        }
    }
}
