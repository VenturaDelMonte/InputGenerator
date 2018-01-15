package de.adrian.thesis.generator.benchmark;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ForwardingThread<T> extends Thread {

    private static final Logger LOG = LogManager.getLogger(ForwardingThread.class);

    private static final Marker THROUGHPUT_MARKER = MarkerManager.getMarker("Throughput");

    private static final String THREAD_NAME = "ForwardingThread";

    private static final int TIMEOUT = 60_000;

    private final BlockingQueue<T> queue;
    private final ProgramFinisher finisher;
    private final ForwardingThreadProperties properties;
    private volatile boolean interrupted = false;
    private long totalRecords, currentRecords;
    private long lastTimestamp = System.currentTimeMillis();
    private ServerSocket socket;

    ForwardingThread(ProgramFinisher finisher, BlockingQueue<T> queue, ForwardingThreadProperties properties) {
        super(THREAD_NAME);
        this.finisher = finisher;
        this.queue = queue;
        this.properties = properties;
    }

    @Override
    public void run() {

        try (ServerSocket socket = new ServerSocket(properties.port)) {
            socket.setSoTimeout(TIMEOUT);
            this.socket = socket;

            LOG.info("Waiting for connections on port {}...", properties.port);

            try (Socket client = socket.accept()) {

                try (PrintWriter outputStream = new PrintWriter(client.getOutputStream())) {

                    while (!interrupted && !outputStream.checkError()) {

                        T record = queue.poll(TIMEOUT, TimeUnit.SECONDS);

                        outputStream.write(record + "\n");
                        outputStream.flush();

                        if (properties.logMessages && totalRecords++ % properties.logMessagesModulo == 0) {
                            LOG.info("ForwardingThread consumed '{}'", record);
                        }

                        // TODO Or use System.nanoTime()? Measure computational overhead
                        currentRecords++;
                        long currentTime = System.currentTimeMillis();

                        if (lastTimestamp + 1_000 < currentTime) {
                            LOG.info(THROUGHPUT_MARKER, "Processed {} records at {}", currentRecords, currentTime);
                            currentRecords = 0;
                            lastTimestamp = currentTime;
                        }
                    }
                }
            }

            LOG.info("ForwardingThread finished");

        } catch (InterruptedIOException exception) {
            LOG.error("Timeout after one minute...");
        } catch (IOException e) {
            LOG.error("Error sending element from queue over socket: {}", e);
        } catch (InterruptedException e) {
            LOG.error("InterruptedException in ForwardingThread: {}", e);
        }

        finisher.finish("ForwardingThread finished");
    }

    void stopConsuming() {
        this.interrupt();
        interrupted = true;
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ForwardingThreadProperties {
        private int port;
        private boolean logMessages = true;
        private int logMessagesModulo = 50;
        private boolean logThroughput;

        public ForwardingThreadProperties setPort(int port) {
            this.port = port;
            return this;
        }

        public ForwardingThreadProperties setLogMessages(boolean logMessages) {
            this.logMessages = logMessages;
            return this;
        }

        public ForwardingThreadProperties setLogMessagesModulo(int logMessagesModulo) {
            this.logMessagesModulo = logMessagesModulo;
            return this;
        }

        public ForwardingThreadProperties setLogThroughput(boolean logThroughput) {
            this.logThroughput = logThroughput;
            return this;
        }
    }
}
