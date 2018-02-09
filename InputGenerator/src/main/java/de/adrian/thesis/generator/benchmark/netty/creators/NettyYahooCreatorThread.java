package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.yahoo.YahooGenerator;

import java.util.Queue;

public class NettyYahooCreatorThread extends AbstractNettyCreatorThread {

    private final static String THREAD_NAME = "NettyYahooCreatorThread";

    public static long WAITING_TIME;

    public static long INITIAL_SEED;

    private final YahooGenerator yahooGenerator;

    public NettyYahooCreatorThread(Queue<String> queue, AbstractNettyCreatorThreadProperties properties) {
        super(THREAD_NAME, queue, properties);
        this.yahooGenerator = new YahooGenerator(properties.maxNumbers / 10, INITIAL_SEED);
    }

    @Override
    String generateRecord() {
        return yahooGenerator.getNext();
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
