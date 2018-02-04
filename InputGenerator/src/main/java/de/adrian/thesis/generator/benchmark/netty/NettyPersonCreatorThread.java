package de.adrian.thesis.generator.benchmark.netty;

import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.nexmark.NexmarkStreamGenerator;
import io.netty.channel.socket.SocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;

/**
 * Generates person instances as csv for the Nexmark benchmark.
 */
public class NettyPersonCreatorThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(NettyPersonCreatorThread.class);

    private final static String THREAD_NAME = "NettyPersonCreatorThread";

    private final NexmarkStreamGenerator nexmarkGenerator;
    private final Queue<String> queue;
    private final NettyPersonCreatorThreadProperties properties;

    private volatile boolean interrupted = false;

    NettyPersonCreatorThread(Queue<String> queue, NettyPersonCreatorThreadProperties properties) {
        super(THREAD_NAME);
        this.queue = queue;
        this.properties = properties;
        this.nexmarkGenerator = NexmarkStreamGenerator.GetInstance();
    }

    @Override
    public void run() {

        // Generate initial numbers
        long counter = 0;
        for (; counter < properties.initialPersons && !interrupted; counter++) {

            String person = nexmarkGenerator.generatePerson();

            queue.add(person);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Initially inserted '{}' into queue", person);
            }
        }

        for (; counter < properties.maxNumbers && !interrupted; counter++) {

            String person = nexmarkGenerator.generatePerson();

            queue.add(person);

            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Inserted '{}' into queue", person);
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

    public static class NettyPersonCreatorThreadProperties extends CreatorThread.CreateThreadProperties {

        int initialPersons;

        public NettyPersonCreatorThreadProperties(CreatorThread.CreateThreadProperties properties) {
            this.delay = properties.delay;
            this.logMessages = properties.logMessages;
            this.logMessagesModulo = properties.logMessagesModulo;
            this.maxNumbers = properties.maxNumbers;
        }

        public NettyPersonCreatorThreadProperties setInitialPersons(int initialPersons) {
            this.initialPersons = initialPersons;
            return this;
        }
    }
}
