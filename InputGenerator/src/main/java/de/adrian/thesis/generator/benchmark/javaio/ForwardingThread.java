package de.adrian.thesis.generator.benchmark.javaio;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ForwardingThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(ForwardingThread.class);

    private static final String THREAD_NAME = "ForwardingThread";

    private static final int WAITING_TIMEOUT = 100;

    private final BlockingQueue<String> queue;
    private final SocketBenchmarkCallback applicationCallback;
    private final ForwardingThreadProperties properties;
    private final CreatorThread producerThread;
    private final AtomicLong currentRecords = new AtomicLong();
    private final ThroughputLoggingThread loggingThread;

    private volatile boolean interrupted = false;
    private long totalRecords;
    private ServerSocket socket;

    ForwardingThread(SocketBenchmarkCallback applicationCallback, BlockingQueue<String> queue, CreatorThread producerThread, ForwardingThreadProperties properties) {
        super(THREAD_NAME);
        this.applicationCallback = applicationCallback;
        this.queue = queue;
        this.producerThread = producerThread;
        this.properties = properties;
        this.loggingThread = new ThroughputLoggingThread(currentRecords, properties.name);
    }

    @Override
    public void run() {

        loggingThread.start();
        listenForClientConnection();

        applicationCallback.finishApplication("ForwardingThread finished");
    }

    private void listenForClientConnection() {
        try (ServerSocket socket = new ServerSocket(properties.port)) {
            socket.setSoTimeout(properties.serverTimeout);
            this.socket = socket;

            LOG.info("Waiting for connections on port {}...", properties.port);

            try (Socket client = socket.accept()) {

                LOG.info("Client connected from {}:{}", client.getInetAddress(), client.getPort());

                try (PrintWriter outputStream = new PrintWriter(client.getOutputStream());
                     Scanner input = new Scanner(client.getInputStream(), "UTF-8").useDelimiter("\n")) {

                    String command = input.next().toLowerCase();
                    handleCommand(command);

                    while (!interrupted && !outputStream.checkError()) {

                        String record = queue.poll(WAITING_TIMEOUT, TimeUnit.MILLISECONDS);

                        if (record == null) {
                            break;
                        }

                        outputStream.write(record + "\n");
                        outputStream.flush();

                        if (properties.logMessages && totalRecords++ % properties.logMessagesModulo == 0) {
                            LOG.info("ForwardingThread consumed '{}'", record);
                        }

                        currentRecords.incrementAndGet();
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
        } catch (Exception exception) {
            LOG.error("GeneralException in ForwardingThread: {}", exception);
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

            if (split.length == 3) {
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
            LOG.error("Exception closing socket in ForwardingThread", e);
        }

        loggingThread.interrupt();
        try {
            loggingThread.join();
        } catch (InterruptedException e) {
            LOG.error("Exception interrupting loggingThread in ForwardingThread", e);
        }
    }

    public static class ForwardingThreadProperties {
        public int port;
        public boolean logMessages;
        public int logMessagesModulo;
        public boolean logThroughput;
        public String name;
        public int serverTimeout;

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

        public ForwardingThreadProperties setName(String name) {
            this.name = name;
            return this;
        }

        public ForwardingThreadProperties setServerTimeout(int serverPort) {
            this.serverTimeout = serverPort;
            return this;
        }
    }
}
