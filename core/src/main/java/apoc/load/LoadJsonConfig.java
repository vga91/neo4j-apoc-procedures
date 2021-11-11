package apoc.load;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LoadJsonConfig extends CommonLoadImportConfig {
    
    private final boolean failOnError;
    private final List<String> pathOptions;

    public LoadJsonConfig(Map<String, Object> config) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        failOnError = (boolean) config.getOrDefault("failOnError", true);
        pathOptions = (List<String>) config.get("pathOptions");
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public List<String> getPathOptions() {
        return pathOptions;
    }
}
