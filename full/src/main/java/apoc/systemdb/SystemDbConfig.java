package apoc.systemdb;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SystemDbConfig {
    public static final String CUSTOM_PROCEDURES_FUNCTIONS = "customProcedures";
    public static final String TRIGGERS = "triggers";
    public static final String UUIDS = "uuids";
    public static final String DV_CATALOGS = "dvCatalogs";
    
    public static final String FEATURES_KEY = "features";
    public static final String FILENAME_KEY = "fileName";

    private final List<String> features;
    private final String fileName;

    public SystemDbConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        List<String> DEFAULT_FEATURES = List.of(CUSTOM_PROCEDURES_FUNCTIONS, TRIGGERS, UUIDS, DV_CATALOGS);
        this.features = (List<String>) config.getOrDefault(FEATURES_KEY, DEFAULT_FEATURES);
        this.fileName = (String) config.getOrDefault(FILENAME_KEY, "metadata");
    }

    public List<String> getFeatures() {
        return features;
    }

    public String getFileName() {
        return fileName;
    }
}
