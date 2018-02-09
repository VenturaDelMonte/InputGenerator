package de.adrian.thesis.generator.yahoo.data;

import java.io.Serializable;

public class CampaignAd implements Serializable {
    public final String adId;
    public final String campaign_id;

    public CampaignAd(String adId, String campaign_id) {
        this.adId = adId;
        this.campaign_id = campaign_id;
    }
}