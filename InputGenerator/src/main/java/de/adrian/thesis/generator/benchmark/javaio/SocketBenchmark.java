package de.adrian.thesis.generator.benchmark.javaio;

import de.adrian.thesis.generator.benchmark.Benchmark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Creates two simples thread, one for creating records and one for serving them to Flink and computing the throughput.
 * For testing, you can create a socket with netcat via {@code nc localhost 9117 -N}
 */
public class SocketBenchmark extends Benchmark implements SocketBenchmarkCallback {

    private static final Logger LOG = LogManager.getLogger(SocketBenchmark.class);

    private CreatorThread<String> creatorThread;
    private ForwardingThread<String> forwardingThread;
    private boolean interrupted = false;

    private boolean clientWillReconnect = false;

    private SocketBenchmark(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        SocketBenchmark socketBenchmark = new SocketBenchmark(args);
        socketBenchmark.registerShutdownHook();
        socketBenchmark.startGenerator();
    }

    @Override
    public void startGenerator() {

        LOG.info("Starting SocketBenchmark on port {} with maxNumberOfMessages {}", port, maxNumberOfMessages);

        BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        CreatorThread.CreateThreadProperties creatorProperties = getCreatorProperties();
        ForwardingThread.ForwardingThreadProperties forwardingProperties = getForwardingProperties();

        do {
            clientWillReconnect = false;
            interrupted = false;

            creatorThread = new CreatorThread<String>(this, queue, recordCreator, creatorProperties);
            forwardingThread = new ForwardingThread<>(this, queue, creatorThread, forwardingProperties);

            forwardingThread.start();

            try {
                forwardingThread.join();
                creatorThread.join();
            } catch (InterruptedException e) {
                LOG.error("SocketBenchmark has been interrupted: {}", e.getLocalizedMessage());
            }
        } while (clientWillReconnect);
    }

    @Override
    public void finishApplication(String reason) {
        if (!interrupted) {
            LOG.info("SocketBenchmark interrupted due to '{}'", reason);
            creatorThread.stopProducing();
            forwardingThread.stopConsuming();
            interrupted = true;
        }
    }

    @Override
    public void waitingForReconnect() {
        clientWillReconnect = true;
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> finishApplication("User aborted program execution")));
    }
}