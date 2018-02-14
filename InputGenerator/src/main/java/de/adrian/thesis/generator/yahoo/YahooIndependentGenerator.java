package de.adrian.thesis.generator.yahoo;

import de.adrian.thesis.generator.yahoo.data.CampaignAd;
import de.adrian.thesis.generator.yahoo.data.Constants;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This generator for the Yahoo-Streaming benchmark generates pairs of AD_ID to CAMPAIGN_ID in advance.
 * May be costly, when generating more that 10 million events.
 * Flink source needs to generate this mapping as well.
 */
public class YahooIndependentGenerator extends YahooBenchmarkGenerator {

    private static final AtomicLong ADVERTISING_ID = new AtomicLong();

    public YahooIndependentGenerator() {
    }

    @Override
    public String getNext() {
        adTypeCounter += 1;
        eventTypeCounter += 1;
        timestampCounter += 1;

        if (adTypeCounter >= AD_TYPE_LENGTH) {
            adTypeCounter = 0;
        }

        if (eventTypeCounter >= EVENT_TYPE_LENGTH) {
            eventTypeCounter = 0;
        }

        // TODO Check if feasible
        if (timestampCounter >= 1000) {
            timestampCounter = 0;
            timestamp = System.currentTimeMillis();
        }

        String adType = Constants.AD_TYPES.get(adTypeCounter);
        String eventType = Constants.EVENT_TYPES.get(eventTypeCounter);

        // ID, randomUser, randomPage, AdId, AdType, AdEvent, Timestamp, IpAddress
        stringBuilder.setLength(0);
        return stringBuilder
                .append(ADVERTISING_ID.getAndIncrement())
                .append(",")
                .append(DUMMY_UUID)
                .append(",")
                .append(DUMMY_UUID)
                .append(",")
                .append(UUID.randomUUID())
                .append(",")
                .append(adType)
                .append(",")
                .append(eventType)
                .append(",")
                .append(timestamp)
                .append(",")
                .append(IP_ADDRESS)
                .toString();
    }
}
