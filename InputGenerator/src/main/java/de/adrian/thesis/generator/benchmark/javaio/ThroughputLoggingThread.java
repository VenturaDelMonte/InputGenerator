package de.adrian.thesis.generator.benchmark.javaio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.concurrent.atomic.AtomicLong;

public class ThroughputLoggingThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(ThroughputLoggingThread.class);

    private static final Marker THROUGHPUT_MARKER = MarkerManager.getMarker("Throughput");
    private final String instanceName;

    private final AtomicLong throughputCount;

    public ThroughputLoggingThread(AtomicLong throughputCount, String instanceName) {
        this.throughputCount = throughputCount;
        this.instanceName = instanceName;
    }

    @Override
    public void run() {

        long currentTime;

        while (true) {
            try {
                Thread.sleep(1000);

                // TODO Or use System.nanoTime()? Measure computational overhead
                currentTime = System.currentTimeMillis();

                LOG.info(THROUGHPUT_MARKER, "{},{},{}", instanceName, throughputCount, currentTime);
                throughputCount.set(0);
            } catch (InterruptedException e) {
                LOG.error("ThroughputLoggingThread was interrupted");
                return;
            }
        }
    }
}
