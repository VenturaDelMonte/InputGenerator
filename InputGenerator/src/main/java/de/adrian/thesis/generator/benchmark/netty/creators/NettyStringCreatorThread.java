package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.benchmark.netty.creators.recordcreator.RecordCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates records using {@link RecordCreator} and inserts them using a queue.
 */
public class NettyStringCreatorThread extends AbstractNettyCreatorThread {

    private static final Logger LOG = LogManager.getLogger(NettyStringCreatorThread.class);

    private final static String THREAD_NAME = "NettyCreatorThread";

    public static long WAITING_DURATION = 10;

    private final RecordCreator recordCreator;

    public NettyStringCreatorThread(RecordCreator recordCreator, AbstractNettyCreatorThreadProperties properties, long startingNumber) {
        super(THREAD_NAME, properties);
        this.recordCreator = recordCreator;
        this.counter = startingNumber;

        if (startingNumber >= properties.maxNumbers) {
            LOG.error("Starting number already bigger that maxNumber");
            throw new IllegalStateException("Starting number already bigger that maxNumber");
        }
    }

    @Override
    String generateRecord(long currentNumber) {
        return recordCreator.createRecord(currentNumber);
    }

    @Override
    long getWaitingDuration() {
        return WAITING_DURATION;
    }

    @Override
    public String getShortDescription() {
        return "NettyStringCreatorThread-" + recordCreator.getClass();
    }
}
