package apoc.text;

import apoc.util.Util;

import java.text.Normalizer;
import java.util.Collections;
import java.util.Map;

public class StringsConfig {
    private final boolean onlyAnum;
    private final Normalizer.Form normalizerForm;

    public StringsConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.onlyAnum = Util.toBoolean(config.getOrDefault("onlyAnum", true));
        final String form = config.getOrDefault("normalizerForm", Normalizer.Form.NFD.name()).toString().toUpperCase();
        this.normalizerForm = Normalizer.Form.valueOf(form);
    }

    public boolean isOnlyAnum() {
        return onlyAnum;
    }

    public Normalizer.Form getNormalizerForm() {
        return normalizerForm;
    }
}
