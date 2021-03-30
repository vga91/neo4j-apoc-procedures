package apoc.load.util;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class LoadArrowConfig {

    private long skip;
    private long limit;
    private boolean failOnError;
    private EnumSet<Results> results;
    private List<String> ignore;

    public LoadArrowConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();

        long skip = (long) config.getOrDefault("skip", 0L);
        this.skip = skip > -1 ? skip : 0L;
        limit = (long) config.getOrDefault("limit", Long.MAX_VALUE);
        failOnError = (boolean) config.getOrDefault("failOnError", true);
        results = EnumSet.noneOf(Results.class);
        List<String> resultList = (List<String>) config.getOrDefault("results", asList("map","list"));
        for (String result : resultList) {
            results.add(Results.valueOf(result));
        }

        ignore = (List<String>) config.getOrDefault("ignore", emptyList());
    }

    public long getSkip() {
        return skip;
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

}
