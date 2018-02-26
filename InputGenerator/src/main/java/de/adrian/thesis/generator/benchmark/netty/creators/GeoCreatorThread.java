package de.adrian.thesis.generator.benchmark.netty.creators;

import de.adrian.thesis.generator.nexmark.NexmarkStreamGenerator;

import java.util.concurrent.ThreadLocalRandom;

public class GeoCreatorThread extends AbstractNettyCreatorThread {

    private final static String THREAD_NAME = "GeoCreatorThread";

    enum Categories {
        CAR, BIKE, PEDESTRIAN, TRUCK
    }

    private ThreadLocalRandom random = ThreadLocalRandom.current();

    private StringBuilder builder = new StringBuilder();
    private int categoryLength = Categories.values().length;

    public GeoCreatorThread(AbstractNettyCreatorThreadProperties creatorProperties) {
        super(THREAD_NAME, creatorProperties);
    }

    @Override
    String generateRecord(long currentNumber) {
        builder
                .append(random.nextLong())
                .append(",")
                .append(random.nextLong())
                .append(",")
                .append(Categories.values()[(int) (currentNumber % categoryLength)].name());

        return builder.toString();
    }

    @Override
    public String getShortDescription() {
        return "geoEvents";
    }
}
