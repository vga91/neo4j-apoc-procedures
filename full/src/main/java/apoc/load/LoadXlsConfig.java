package apoc.load;

import apoc.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static apoc.load.LoadXls.DEFAULT_ARRAY_SEP;
import static apoc.util.Util.parseCharFromConfig;

public class LoadXlsConfig extends LoadImportConfig {

    private final boolean failOnError;
    private char arraySep;
    private long skip;
    private boolean hasHeader;
    private long limit;

    public LoadXlsConfig(Map<String, Object> config) {
        // zoneId = null not to break changes and fails with dateTime parse without zone id (testLoadXlsDateWithMappingArrayTypeZoneDateTimeWithError)
        // but maybe might be worth provide a default value via db.temporal.timezone
        super(config, null);
        if (config == null) {
            config = Collections.emptyMap();
        }
        arraySep = parseCharFromConfig(config, "arraySep", DEFAULT_ARRAY_SEP);
        skip = Util.toLong(config.getOrDefault("skip", 0L));
        hasHeader = Util.toBoolean(config.getOrDefault( "header", true));
        limit = Util.toLong(config.getOrDefault("limit", Long.MAX_VALUE));
        failOnError = Util.toBoolean(config.getOrDefault( "failOnError", true));
    }

    public char getArraySep() {
        return arraySep;
    }

    public long getSkip() {
        return skip;
    }

    public boolean hasHeader() {
        return hasHeader;
    }

    public long getLimit() {
        return limit;
    }

    public boolean isFailOnError() {
        return failOnError;
    }
    
    @Override
    public Map<String, XlsMapping> createMapping(Object ignored) {
        final Map<String, Map<String, Object>> mapping = getMapping();
        if (mapping.isEmpty()) return Collections.emptyMap();
        HashMap<String, XlsMapping> result = new HashMap<>(mapping.size());
        for (Map.Entry<String, Map<String, Object>> entry : mapping.entrySet()) {
            String name = entry.getKey();
            result.put(name, new XlsMapping(name, this));
        }
        return result;
    }
}
