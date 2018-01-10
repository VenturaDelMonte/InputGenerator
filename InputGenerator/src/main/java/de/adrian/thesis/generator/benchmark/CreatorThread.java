package de.adrian.thesis.generator.benchmark;

import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class CreatorThread<T> extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(CreatorThread.class);

    private final static String THREAD_NAME = "CreatorThread";

    private final Queue<T> queue;
    private final RecordCreator<T> recordCreator;
    private final CreateThreadProperties properties;
    private final ProgramFinisher finisher;
    private volatile boolean interrupted = false;

    CreatorThread(ProgramFinisher finisher,
                  Queue<T> queue,
                  RecordCreator<T> recordCreator) {
        super(THREAD_NAME);
        this.finisher = finisher;
        this.queue = queue;
        this.recordCreator = recordCreator;
        this.properties = new CreateThreadProperties();
    }

    CreatorThread(ProgramFinisher finisher,
                  Queue<T> queue,
                  RecordCreator<T> recordCreator,
                  CreateThreadProperties properties) {
        super(THREAD_NAME);
        this.finisher = finisher;
        this.queue = queue;
        this.recordCreator = recordCreator;
        this.properties = properties;
    }

    @Override
    public void run() {
        for (long counter = 0; counter != properties.maxNumbers && !interrupted; counter++) {
            T record = recordCreator.createRecord(counter);
            queue.add(record);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Inserted '{}' into queue", record);
            }

            try {
                Thread.sleep(properties.delay);
            } catch (InterruptedException e) {
                LOG.error("CreatorThread was interrupted: {}", e.getLocalizedMessage());
            }
        }

        finisher.finish("Creator send all records");
    }

    void stopProducing() {
        interrupt();
        interrupted = true;
    }

    static class CreateThreadProperties {
        private int delay = 50;
        private long maxNumbers = -1;
        private boolean logMessages = true;
        private int logMessagesModulo = 50;

        CreateThreadProperties setDelay(int delay) {
            this.delay = delay;
            return this;
        }

        CreateThreadProperties setMaxNumbers(long maxNumbers) {
            this.maxNumbers = maxNumbers;
            return this;
        }

        CreateThreadProperties setLogMessages(boolean logMessages) {
            this.logMessages = logMessages;
            return this;
        }

        CreateThreadProperties setLogMessagesModulo(int logMessagesModulo) {
            this.logMessagesModulo = logMessagesModulo;
            return this;
        }
    }
}
