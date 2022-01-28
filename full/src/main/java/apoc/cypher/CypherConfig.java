package apoc.cypher;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class CypherConfig {
    private final Map<String, Object> params;
    private final boolean sameColumns;
    private final long timeout;
    private final boolean statistics;

    public CypherConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.params = (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
        this.sameColumns = Util.toBoolean(config.getOrDefault("sameColumns", true));
        this.statistics = Util.toBoolean(config.get("statistics"));
        this.timeout = Util.toLong(config.getOrDefault("timeout", 10L));
    }

    public Map<String, Object> getParams() {
        return params;
    }


    public boolean isSameColumns() {
        return sameColumns;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isStatistics() {
        return statistics;
    }
}
