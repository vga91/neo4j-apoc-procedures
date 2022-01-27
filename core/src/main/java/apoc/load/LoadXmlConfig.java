package apoc.load;

import java.util.Collections;
import java.util.Map;

public class LoadXmlConfig extends LoadImportConfig {
    
    private boolean failOnError;
    private Map<String, Object> headers;
    
    public LoadXmlConfig(Map<String, Object> config) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        failOnError = (boolean) config.getOrDefault("failOnError", true);
        headers = (Map) config.getOrDefault( "headers", Collections.emptyMap() );;
    }

    @Override
    public Object createMapping(Object input) {
        return null;
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }
}
