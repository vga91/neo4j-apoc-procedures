package apoc.es;

import apoc.load.ConnectionConfig;

import java.util.Map;

public class ElasticConfig extends ConnectionConfig {
    public static final String HEADER_KEY = "header";

    private final Map<String, Object> header;

    public ElasticConfig(Map<String, Object> config) {
        super(config);
        this.header = (Map<String, Object>) config.get(HEADER_KEY);
    }

    public Map<String, Object> getHeader() {
        return header;
    }
    
    
}
