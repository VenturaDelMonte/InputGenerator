package de.adrian.thesis.generator.yahoo;

import de.adrian.thesis.generator.yahoo.data.CampaignAd;
import de.adrian.thesis.generator.yahoo.data.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * This generator for the Yahoo-Streaming benchmark generates pairs of AD_ID to CAMPAIGN_ID in advance.
 * May be costly, when generating more that 10 million events.
 * Flink source needs to generate this mapping as well.
 */
public class YahooMatchingUUIDGenerator extends YahooBenchmarkGenerator {

    private static final int NUMBER_OF_ADS_PER_CAMPAIGN = 10;

    private final StringBuilder stringBuilder = new StringBuilder(100);

    private final CampaignAd[] campaingsArray;
    private final int campaignLength;

    private static List<CampaignAd> GenerateCampaignMapping(long numCampaigns, long seed) {

        Random random = new Random(seed);

        byte[] bytes = new byte[7];

        List<CampaignAd> campaignAds = new ArrayList<>();

        for (int i = 0; i < numCampaigns; i++) {

            random.nextBytes(bytes);
            String campaign = UUID.nameUUIDFromBytes(bytes).toString();

            for (int j = 0; j < NUMBER_OF_ADS_PER_CAMPAIGN; j++) {
                random.nextBytes(bytes);
                campaignAds.add(new CampaignAd(UUID.nameUUIDFromBytes(bytes).toString(), campaign));
            }
        }

        return campaignAds;
    }

    public YahooMatchingUUIDGenerator(long numberOfCampaigns, long seed) {
        List<CampaignAd> campaignAds = GenerateCampaignMapping(numberOfCampaigns, seed);

        this.campaingsArray = campaignAds.toArray(new CampaignAd[campaignAds.size()]);
        this.campaignLength = this.campaingsArray.length;
    }

    @Override
    public String getNext() {
        campaignLengthCounter += 1;
        adTypeCounter += 1;
        eventTypeCounter += 1;
        timestampCounter += 1;

        if (campaignLengthCounter >= campaignLength) {
            campaignLengthCounter = 0;
        }

        if (adTypeCounter >= AD_TYPE_LENGTH) {
            adTypeCounter = 0;
        }

        if (eventTypeCounter >= EVENT_TYPE_LENGTH) {
            eventTypeCounter = 0;
        }

        if (timestampCounter >= 1000) {
            timestampCounter = 0;
            timestamp = System.currentTimeMillis();
        }

        String adId = campaingsArray[campaignLengthCounter].adId;
        String adType = Constants.AD_TYPES.get(adTypeCounter);
        String eventType = Constants.EVENT_TYPES.get(eventTypeCounter);

        stringBuilder.setLength(0);
        return stringBuilder
                .append(DUMMY_UUID)
                .append(",")
                .append(DUMMY_UUID)
                .append(",")
                .append(adId)
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
