package de.adrian.thesis.generator.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.adrian.thesis.generator.benchmark.recordcreator.CountingRecordCreator;
import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class SocketBenchmark implements ProgramFinisher {

    private static final Logger LOG = LoggerFactory.getLogger(SocketBenchmark.class);

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
    private int maxNumberOfMessages = 50;

    public static void main(String[] args) throws Exception {
        SocketBenchmark socketBenchmark = new SocketBenchmark();
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
        BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        RecordCreator recordCreator = chooseRecordCreator(this.recordCreator);

        CreatorThread.CreateThreadProperties properties =
                new CreatorThread.CreateThreadProperties(msDelay, maxNumberOfMessages);

        creatorThread = new CreatorThread<String>(this, queue, recordCreator, properties);

        forwardingThread = new ForwardingThread<>(this, queue, port);

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
            LOG.info("SocketBenchmark interrupted due to '{}'" , reason);
            creatorThread.stopProducing();
            forwardingThread.stopConsuming();
            interrupted = true;
        }
    }
}