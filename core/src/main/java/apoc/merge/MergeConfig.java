package apoc.merge;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MergeConfig {

    private final List<String> mergeKeysList;
    private final Map<String, Object> onMatch;
    private final Map<String, Object> onCreate;

    public MergeConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.mergeKeysList = (List<String>) config.getOrDefault("mergeKeysList", Collections.emptyList());
        this.onMatch = (Map<String, Object>) config.getOrDefault("onMatch", Collections.emptyMap());
        this.onCreate = (Map<String, Object>) config.getOrDefault("onCreate", Collections.emptyMap());
    }

    public Map<String, Object> getOnMatch() {
        return onMatch;
    }

    public Map<String, Object> getOnCreate() {
        return onCreate;
    }

    public List<String> getMergeKeysList() {
        return mergeKeysList;
    }
}
