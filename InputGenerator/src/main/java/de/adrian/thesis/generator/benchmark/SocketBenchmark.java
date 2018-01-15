package de.adrian.thesis.generator.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.adrian.thesis.generator.benchmark.recordcreator.CountingRecordCreator;
import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Creates two simples thread, one for creating records and one for serving them to Flink and computing the throughput.
 * For testing, you can create a socket with netcat via {@code nc localhost 9117 -N}
 */
public class SocketBenchmark implements ProgramFinisher {

    private static final Logger LOG = LogManager.getLogger(SocketBenchmark.class);

    private static final Map<String, RecordCreator> GENERATORS = new HashMap<>(4);

    private CreatorThread<String> creatorThread;
    private ForwardingThread<String> forwardingThread;
    private boolean interrupted = false;

    static {
        GENERATORS.put("numbers", new CountingRecordCreator());
    }

    @Parameter(names = {"-p", "--port"}, description = "Specifies the port for this sender")
    private int port = 9117;

    @Parameter(names = {"-c", "--creator"}, description = "Specifies the record creator this sender should use")
    private String recordCreator = "numbers";

    @Parameter(names = {"-d", "--delay"}, description = "Delay between between insertions of new elements to queue")
    private int msDelay = 50;

    @Parameter(names = {"-m", "--maxMessages"}, description = "Max number of messages, that should be sent")
    private int maxNumberOfMessages = 500;

    @Parameter(names = {"-l", "--logMessages"}, description = "Displays log messages for the creation and sending progress")
    private boolean logMessages = true;

    @Parameter(names = {"-mo", "--logModulo"}, description = "Only display log messages for only ever n-th record")
    private int logMessagesModulo = 50;

    @Parameter(names = {"-t", "--throughput"}, description = "Logs throughput for ForwardingThread")
    private boolean throughput = true;

    public static void main(String[] args) throws Exception {
        SocketBenchmark socketBenchmark = new SocketBenchmark();
        socketBenchmark.registerShutdownHook();
        socketBenchmark.parseCLI(args);
        socketBenchmark.startGenerator();
    }

    private void parseCLI(String[] args) {
        JCommander.newBuilder()
                .addObject(this)
                .acceptUnknownOptions(true)
                .build()
                .parse(args);
    }

    private void startGenerator() {

        LOG.info("Starting SocketBenchmark on port {} with maxNumberOfMessages {}", port, maxNumberOfMessages);

        BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        RecordCreator recordCreator = chooseRecordCreator(this.recordCreator);

        CreatorThread.CreateThreadProperties creatorProperties = new CreatorThread.CreateThreadProperties();
        creatorProperties
                .setDelay(msDelay)
                .setMaxNumbers(maxNumberOfMessages)
                .setLogMessages(logMessages)
                .setLogMessagesModulo(logMessagesModulo);

        creatorThread = new CreatorThread<String>(this, queue, recordCreator, creatorProperties);

        ForwardingThread.ForwardingThreadProperties forwardingProperties = new ForwardingThread.ForwardingThreadProperties();
        forwardingProperties
                .setPort(port)
                .setLogThroughput(throughput)
                .setLogMessages(logMessages)
                .setLogMessagesModulo(logMessagesModulo);

        forwardingThread = new ForwardingThread<>(this, queue, forwardingProperties);

        creatorThread.start();
        forwardingThread.start();

        try {
            creatorThread.join();
            forwardingThread.join();
        } catch (InterruptedException e) {
            LOG.error("SocketBenchmark has been interrupted: {}", e.getLocalizedMessage());
        }
    }

    private RecordCreator chooseRecordCreator(String name) {
        RecordCreator recordCreator = GENERATORS.get(name);

        if (recordCreator == null) {
            LOG.error("Generator '{}' could not be found", name);
            throw new IllegalArgumentException("Generator '" + name + "' could not be found");
        } else {
            return recordCreator;
        }
    }

    @Override
    public void finish(String reason) {
        if (!interrupted) {
            LOG.info("SocketBenchmark interrupted due to '{}'", reason);
            creatorThread.stopProducing();
            forwardingThread.stopConsuming();
            interrupted = true;
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> finish("User aborted program execution")));
    }
}