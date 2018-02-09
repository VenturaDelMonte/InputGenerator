package de.adrian.thesis.generator.yahoo.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Static variables used through out the benchmark.
 */
public class Constants implements Serializable {

    public static final List<String> AD_TYPES = Arrays.asList("banner", "modal", "sponsored-search", "mail", "mobile");
    public static final List<String> EVENT_TYPES = Arrays.asList("view", "click", "purchase");

    private Constants() {
    }
}
