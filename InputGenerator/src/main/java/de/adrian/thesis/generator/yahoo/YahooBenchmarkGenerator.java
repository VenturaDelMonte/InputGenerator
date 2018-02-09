package de.adrian.thesis.generator.yahoo;

import de.adrian.thesis.generator.yahoo.data.Constants;

import java.util.UUID;

public abstract class YahooBenchmarkGenerator {

    static final String DUMMY_UUID = UUID.randomUUID().toString();
    static final String IP_ADDRESS = "255.255.255.255";

    static final int AD_TYPE_LENGTH = Constants.AD_TYPES.size();
    static final int EVENT_TYPE_LENGTH = Constants.EVENT_TYPES.size();

    final StringBuilder stringBuilder = new StringBuilder(100);

    int campaignLengthCounter = 0, adTypeCounter = 0, eventTypeCounter = 0, timestampCounter = 0;
    long timestamp = System.currentTimeMillis();

    public abstract String getNext();
}
