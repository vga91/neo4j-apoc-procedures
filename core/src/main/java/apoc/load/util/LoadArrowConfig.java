package apoc.load.util;

import apoc.load.Mapping;
import apoc.util.Util;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class LoadArrowConfig {

    private final boolean ignoreErrors;
    private long skip;
    private boolean hasHeader;
    private long limit;

    private boolean failOnError;
    private boolean ignoreQuotations;

    private EnumSet<Results> results;

    private List<String> ignore;

    public LoadArrowConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();

        ignoreErrors = Util.toBoolean(config.getOrDefault("ignoreErrors", false));
        long skip = (long) config.getOrDefault("skip", 0L);
        this.skip = skip > -1 ? skip : 0L;
        hasHeader = (boolean) config.getOrDefault("header", true);
        limit = (long) config.getOrDefault("limit", Long.MAX_VALUE);
        failOnError = (boolean) config.getOrDefault("failOnError", true);
        ignoreQuotations = (boolean) config.getOrDefault("ignoreQuotations", false);

        results = EnumSet.noneOf(Results.class);
        List<String> resultList = (List<String>) config.getOrDefault("results", asList("map","list"));
        for (String result : resultList) {
            results.add(Results.valueOf(result));
        }

        ignore = (List<String>) config.getOrDefault("ignore", emptyList());
    }

    private Map<String, Mapping> createMapping(Map<String, Map<String, Object>> mapping, char arraySep, List<String> ignore) {
        if (mapping.isEmpty()) return Collections.emptyMap();
        HashMap<String, Mapping> result = new HashMap<>(mapping.size());
        for (Map.Entry<String, Map<String, Object>> entry : mapping.entrySet()) {
            String name = entry.getKey();
            result.put(name, new Mapping(name, entry.getValue(), arraySep, ignore.contains(name)));
        }
        return result;
    }

    public long getSkip() {
        return skip;
    }

    public boolean isHasHeader() {
        return hasHeader;
    }

    public long getLimit() {
        return limit;
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public EnumSet<Results> getResults() {
        return results;
    }

    public List<String> getIgnore() {
        return ignore;
    }

    public boolean getIgnoreErrors() {
        return ignoreErrors;
    }

    public boolean isIgnoreQuotations() {
        return ignoreQuotations;
    }
}
