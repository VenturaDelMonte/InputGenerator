package de.adrian.thesis.generator.benchmark.javaio;

import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;

public class CreatorThread<T> extends Thread {

    private static final Logger LOG = LogManager.getLogger(CreatorThread.class);

    private final static String THREAD_NAME = "CreatorThread";

    private final Queue<T> queue;
    private final RecordCreator<T> recordCreator;
    private final CreateThreadProperties properties;
    private final SocketBenchmarkCallback finisher;
    private long startingNumber = 0;
    private volatile boolean interrupted = false;

    CreatorThread(SocketBenchmarkCallback finisher,
                  Queue<T> queue,
                  RecordCreator<T> recordCreator,
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
            T record = recordCreator.createRecord(counter);
            queue.add(record);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Inserted '{}' into queue", record);
            }

            try {
                Thread.sleep(properties.delay);
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
        public int delay = 50;
        public long maxNumbers = -1;
        public boolean logMessages = true;
        public int logMessagesModulo = 50;

        public CreateThreadProperties setDelay(int delay) {
            this.delay = delay;
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
