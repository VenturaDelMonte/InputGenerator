package de.adrian.thesis.generator.benchmark.netty;

import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import io.netty.channel.socket.SocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Creates records using {@link RecordCreator<T>} and inserts them using a queue.
 * The {@link NettyForwardingThread<T>} will forward the records using a socket.
 *
 * @param <T> the record and queue type
 */
public class NettyCreatorThread<T> extends Thread {

    private static final Logger LOG = LogManager.getLogger(NettyCreatorThread.class);

    private final static String THREAD_NAME = "NettyCreatorThread";

    private SocketChannel channel;
    private final Queue<T> queue;
    private final RecordCreator<T> recordCreator;
    private final CreatorThread.CreateThreadProperties properties;

    private long startingNumber = 0;
    private volatile boolean interrupted = false;

    NettyCreatorThread(SocketChannel channel, BlockingQueue<T> queue, RecordCreator<T> recordCreator, CreatorThread.CreateThreadProperties properties) {
        super(THREAD_NAME);
        this.channel = channel;
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
            LOG.error("Starting number already bigger that maxNumber");
            channel.close();
            return;
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
                LOG.error("NettyCreatorThread was interrupted: {}", e.getLocalizedMessage());
                break;
            }
        }
    }

    void stopProducing() {
        interrupt();
        interrupted = true;
    }
}
