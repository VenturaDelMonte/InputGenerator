package de.adrian.thesis.generator.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.adrian.thesis.generator.benchmark.recordcreator.CountingRecordCreator;
import de.adrian.thesis.generator.benchmark.recordcreator.CountingTimestampRecordCreator;
import de.adrian.thesis.generator.benchmark.recordcreator.RecordCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public abstract class Benchmark<T> {

    private static final Logger LOG = LogManager.getLogger(Benchmark.class);

    private static final Map<String, RecordCreator> GENERATORS = new HashMap<>(4);

    static {
        GENERATORS.put("numbers", new CountingRecordCreator());
        GENERATORS.put("timestamp", new CountingTimestampRecordCreator());
    }

    @Parameter(names = {"-p", "--port"}, description = "Specifies the port for this sender")
    protected int port = 9117;

    @Parameter(names = {"-c", "--creator"}, description = "Specifies the record creator this sender should use")
    private String recordCreatorName = "timestamp";

    @Parameter(names = {"-d", "--delay"}, description = "Delay between between insertions of new elements to queue")
    protected int msDelay = 50;

    @Parameter(names = {"-m", "--maxMessages"}, description = "Max number of messages, that should be sent")
    protected int maxNumberOfMessages = 500;

    @Parameter(names = {"-l", "--logMessages"}, description = "Displays log messages for the creation and sending progress")
    protected boolean logMessages = true;

    @Parameter(names = {"-mo", "--logModulo"}, description = "Only display log messages for only ever n-th record")
    protected int logMessagesModulo = 50;

    @Parameter(names = {"-t", "--throughput"}, description = "Logs throughput for ForwardingThread")
    protected boolean throughput = true;

    @Parameter(names = {"-n", "--name"}, description = "Assigns name to this producing instance. Useful for logging throughput.")
    protected String name = "DefaultInstance";

    protected RecordCreator<T> recordCreator;

    public Benchmark(String[] args) {
        Benchmark benchmark = getInstance();
        benchmark.parseCLI(args);
        benchmark.reconfigureLoggerForDynamicFilename();
        recordCreator = benchmark.chooseRecordCreator(recordCreatorName);
    }

    public abstract Benchmark getInstance();

    public abstract void startGenerator();

    private void parseCLI(String[] args) {
        JCommander.newBuilder()
                .addObject(getInstance())
                .acceptUnknownOptions(true)
                .build()
                .parse(args);
    }

    private RecordCreator<T> chooseRecordCreator(String name) {
        RecordCreator<T> recordCreator = GENERATORS.get(name);

        if (recordCreator == null) {
            LOG.error("Generator '{}' could not be found", name);
            throw new IllegalArgumentException("Generator '" + name + "' could not be found");
        } else {
            return recordCreator;
        }
    }

    private void reconfigureLoggerForDynamicFilename() {

        System.setProperty("instanceName", name);

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();

        try {
            File obsoleteThroughputLogFile = new File("logs/${sys:instanceName}-throughput.log");
            Files.delete(obsoleteThroughputLogFile.toPath());

            File obsoleteStdoutLogFile = new File("logs/${sys:instanceName}-stdout.log");
            Files.delete(obsoleteStdoutLogFile.toPath());
        } catch (IOException exception) {
            LOG.error("Dummy log files have already been deleted.");
        }
    }
}