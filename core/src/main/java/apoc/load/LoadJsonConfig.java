package apoc.load;

import java.util.Collections;
import java.util.Map;

public class LoadJsonConfig extends CommonLoadImportConfig {
    
    private boolean failOnError;
    
    public LoadJsonConfig(Map<String, Object> config) {
        super(config);
        // todo - config null needed?
        if (config == null) {
            config = Collections.emptyMap();
        }
        failOnError = (boolean) config.getOrDefault("failOnError", true);
    }

    public boolean isFailOnError() {
        return failOnError;
    }
}
