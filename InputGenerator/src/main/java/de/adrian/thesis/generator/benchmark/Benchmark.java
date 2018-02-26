package de.adrian.thesis.generator.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import de.adrian.thesis.generator.benchmark.javaio.ForwardingThread;
import de.adrian.thesis.generator.benchmark.netty.creators.recordcreator.CountingRecordCreator;
import de.adrian.thesis.generator.benchmark.netty.creators.recordcreator.CountingTimestampRecordCreator;
import de.adrian.thesis.generator.benchmark.netty.creators.recordcreator.RecordCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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

    @Parameter(names = {"-s", "--messagesPerSecond"}, description = "The number of messages per second")
    protected int messagesPerSecond = 50;

    @Parameter(names = {"-m", "--maxMessages"}, description = "Max number of messages, that should be sent")
    protected long maxNumberOfMessages = 500;

    @Parameter(names = {"-l", "--logMessages"}, description = "Displays log messages for the creation and sending progress")
    private boolean logMessages = false;

    @Parameter(names = {"-mo", "--logModulo"}, description = "Only display log messages for only ever n-th record")
    private int logMessagesModulo = 50;

    @Parameter(names = {"-t", "--throughput"}, description = "Logs throughput for ForwardingThread")
    private boolean throughput = true;

    @Parameter(names = {"-n", "--name"}, description = "Assigns name to this producing instance. Useful for logging throughput.")
    protected String name = "DefaultInstance";

    @Parameter(names = {"-st", "--serverTimeout"}, description = "Timeout after which the netty server will shutdown, if no new client has connected and no client is active")
    protected int serverTimeout = 120;

    protected RecordCreator recordCreator;

    public Benchmark(String[] args) {
        parseCLI(args);
        addHostnameToInstanceName();
        reconfigureLoggerForDynamicFilename();
        recordCreator = chooseRecordCreator(recordCreatorName);
    }

    private void addHostnameToInstanceName() {
        try {
            name = name + "@" + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            name = name + "@" + "UnkownHost";
        }
    }

    public abstract void startGenerator();

    /**
     * Do again, if subclass declares additional arguments
     * @param args The command line arguments
     */
    public void parseCLI(String[] args) {
        JCommander.newBuilder()
                .addObject(this)
                .acceptUnknownOptions(true)
                .build()
                .parse(args);
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

    protected CreatorThread.CreateThreadProperties getCreatorProperties() {
        CreatorThread.CreateThreadProperties creatorProperties = new CreatorThread.CreateThreadProperties();

        return creatorProperties
                .setMessagesPerSecond(messagesPerSecond)
                .setMaxNumbers(maxNumberOfMessages)
                .setLogMessages(logMessages)
                .setLogMessagesModulo(logMessagesModulo);
    }

    protected ForwardingThread.ForwardingThreadProperties getForwardingProperties() {
        ForwardingThread.ForwardingThreadProperties forwardingProperties = new ForwardingThread.ForwardingThreadProperties();

        return forwardingProperties
                .setPort(port)
                .setLogThroughput(throughput)
                .setLogMessages(logMessages)
                .setLogMessagesModulo(logMessagesModulo)
                .setName(name)
                .setServerTimeout(serverTimeout);
    }

    private void reconfigureLoggerForDynamicFilename() {

        System.setProperty("instanceName", name);

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();

        try {
            File obsoleteThroughputLogFile = new File("/tmp/bartnik/generator/${sys:instanceName}-throughput.log");
            Files.delete(obsoleteThroughputLogFile.toPath());

            File obsoleteStdoutLogFile = new File("/tmp/bartnik/generator/${sys:instanceName}-stdout.log");
            Files.delete(obsoleteStdoutLogFile.toPath());
        } catch (IOException exception) {
            LOG.error("Dummy log files have already been deleted.");
        }
    }
}