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
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ForwardingThread<T> extends Thread {

    private static final Logger LOG = LogManager.getLogger(ForwardingThread.class);

    private static final Marker THROUGHPUT_MARKER = MarkerManager.getMarker("Throughput");

    private static final String THREAD_NAME = "ForwardingThread";

    private static final int TIMEOUT = 120_000;

    private final BlockingQueue<T> queue;
    private final SocketBenchmarkCallback applicationCallback;
    private final ForwardingThreadProperties properties;
    private final CreatorThread<T> producerThread;
    private volatile boolean interrupted = false;
    private long totalRecords, currentRecords;
    private long lastTimestamp = System.currentTimeMillis();
    private ServerSocket socket;

    ForwardingThread(SocketBenchmarkCallback applicationCallback, BlockingQueue<T> queue, CreatorThread<T> producerThread, ForwardingThreadProperties properties) {
        super(THREAD_NAME);
        this.applicationCallback = applicationCallback;
        this.queue = queue;
        this.producerThread = producerThread;
        this.properties = properties;
    }

    @Override
    public void run() {

        listenForClientConnection();

        applicationCallback.finishApplication("ForwardingThread finished");
    }

    private void listenForClientConnection() {
        try (ServerSocket socket = new ServerSocket(properties.port)) {
            socket.setSoTimeout(TIMEOUT);
            this.socket = socket;

            LOG.info("Waiting for connections on port {}...", properties.port);

            try (Socket client = socket.accept()) {

                LOG.info("Client connected from {}:{}", client.getInetAddress(), client.getPort());

                try (PrintWriter outputStream = new PrintWriter(client.getOutputStream());
                     Scanner input = new Scanner(client.getInputStream(), "UTF-8").useDelimiter("\n")) {

                    String command = input.next().toLowerCase();
                    handleCommand(command);

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
    }

    private void handleCommand(String command) {

        boolean clientWillReconnect = false;

        if (command.startsWith("start")) {

            String[] split = command.split(":");

            if (split.length >= 2) {
                clientWillReconnect = split[1].toLowerCase().contains("reconnect");
            }

            producerThread.start(0);
        } else if (command.startsWith("from:")) {
            String[] split = command.split(":");
            long startingNumber = Long.parseLong(split[1]);
            producerThread.start(startingNumber);

            if (split.length >= 3) {
                clientWillReconnect = split[2].toLowerCase().contains("reconnect");
            }
        } else {
            throw new IllegalArgumentException("Received illegal command from client: " + command);
        }

        if (clientWillReconnect) {
            applicationCallback.waitingForReconnect();
        }
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
