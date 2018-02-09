package de.adrian.thesis.generator.yahoo;

import de.adrian.thesis.generator.yahoo.data.CampaignAd;
import de.adrian.thesis.generator.yahoo.data.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class YahooGenerator {

    private static final String DUMMY_UUID = UUID.randomUUID().toString();
    private static final String IP_ADDRESS = "255.255.255.255";

    private static final int AD_TYPE_LENGTH = Constants.AD_TYPES.size();
    private static final int EVENT_TYPE_LENGTH = Constants.EVENT_TYPES.size();
    private static final int NUMBER_OF_ADS_PER_CAMPAIGN = 10;

    private final StringBuilder stringBuilder = new StringBuilder(100);

    private final CampaignAd[] campaingsArray;
    private final int campaignLength;

    private int campainLengthCounter = 0, adTypeCounter = 0, eventTypeCounter = 0, timestampCounter = 0;
    private long timestamp = System.currentTimeMillis();

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

    public YahooGenerator(long numberOfCampaigns, long seed) {
        List<CampaignAd> campaignAds = GenerateCampaignMapping(numberOfCampaigns, seed);

        this.campaingsArray = campaignAds.toArray(new CampaignAd[campaignAds.size()]);
        this.campaignLength = this.campaingsArray.length;
    }

    public String getNext() {
        campainLengthCounter += 1;
        adTypeCounter += 1;
        eventTypeCounter += 1;
        timestampCounter += 1;

        if (campainLengthCounter >= campaignLength) {
            campainLengthCounter = 0;
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

        String adId = campaingsArray[campainLengthCounter].adId;
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
