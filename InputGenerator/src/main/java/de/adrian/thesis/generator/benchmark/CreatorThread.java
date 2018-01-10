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

            LOG.info("Inserted '{}' into queue", record);

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
        private final int delay;
        private final long maxNumbers;

        CreateThreadProperties() {
            this(100);
        }

        CreateThreadProperties(int delay) {
            this(delay, -1);
        }

        CreateThreadProperties(int delay, long maxNumbers) {
            this.delay = delay;
            this.maxNumbers = maxNumbers;
        }
    }
}
