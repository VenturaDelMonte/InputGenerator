package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.yahoo.YahooBenchmarkGenerator;
import de.adrian.thesis.generator.yahoo.YahooIndependentGenerator;
import de.adrian.thesis.generator.yahoo.YahooMatchingUUIDGenerator;

import java.util.Queue;

public class NettyYahooCreatorThread extends AbstractNettyCreatorThread {

    private final static String THREAD_NAME = "NettyYahooCreatorThread";

    public static long WAITING_TIME;

    public static long INITIAL_SEED;

    public static String GENERATOR_NAME;

    private final YahooBenchmarkGenerator yahooBenchmarkGenerator;

    public NettyYahooCreatorThread(Queue<String> queue, AbstractNettyCreatorThreadProperties properties) {
        super(THREAD_NAME, queue, properties);
        if (GENERATOR_NAME.toLowerCase().contains("independent")) {
            this.yahooBenchmarkGenerator = new YahooIndependentGenerator();
        } else {
            this.yahooBenchmarkGenerator =
                    new YahooMatchingUUIDGenerator(properties.maxNumbers / 10, INITIAL_SEED);
        }
    }

    @Override
    String generateRecord() {
        return yahooBenchmarkGenerator.getNext();
    }

    @Override
    long getWaitingDuration() {
        return WAITING_TIME;
    }

    @Override
    public String getShortDescription() {
        return "YahooBenchmark";
    }
}
