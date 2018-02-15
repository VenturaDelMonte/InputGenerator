package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.yahoo.YahooBenchmarkGenerator;
import de.adrian.thesis.generator.yahoo.YahooIndependentGenerator;
import de.adrian.thesis.generator.yahoo.YahooMatchingUUIDGenerator;

import java.util.Queue;

public class NettyYahooCreatorThread extends AbstractNettyCreatorThread {

    private final static String THREAD_NAME = "NettyYahooCreatorThread";

    public static long INITIAL_SEED;

    public static String GENERATOR_NAME;

    public static long NUMBER_OF_CAMPAIGNS;

    private final YahooBenchmarkGenerator yahooBenchmarkGenerator;

    public NettyYahooCreatorThread(AbstractNettyCreatorThreadProperties properties) {
        super(THREAD_NAME, properties);
        if (GENERATOR_NAME.toLowerCase().equals("independent")) {
            this.yahooBenchmarkGenerator = new YahooIndependentGenerator();
        } else {
            this.yahooBenchmarkGenerator =
                    new YahooMatchingUUIDGenerator(NUMBER_OF_CAMPAIGNS, INITIAL_SEED);
        }
    }

    @Override
    String generateRecord(long currentNumber) {
        return yahooBenchmarkGenerator.getNext();
    }

    @Override
    public String getShortDescription() {
        return "YahooBenchmark";
    }
}
