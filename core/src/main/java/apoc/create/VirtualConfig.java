package apoc.create;

import apoc.util.Util;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VirtualConfig {

    private final boolean merge;
    private final Map<String, Object> onMatch;
    private final Map<String, Object> onCreate;

    public VirtualConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.merge = Util.toBoolean(config.get("merge"));
        this.onMatch = (Map<String, Object>) config.getOrDefault("onMatch", Collections.emptyMap());
        this.onCreate = (Map<String, Object>) config.getOrDefault("onCreate", Collections.emptyMap());
    }

    public boolean isMerge() {
        return merge;
    }

    public Map<String, Object> getOnMatch() {
        return onMatch;
    }

    public Map<String, Object> getOnCreate() {
        return onCreate;
    }
}
