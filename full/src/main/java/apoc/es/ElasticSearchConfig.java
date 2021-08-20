package apoc.es;

import java.util.Collections;
import java.util.Map;

public class ElasticSearchConfig {

    private final Map<String, Object> header;

    public ElasticSearchConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.header = (Map<String, Object>) config.getOrDefault("header", Collections.emptyMap());
    }

    public Map<String, Object> getHeader() {
        return header;
    }

    public Map<String, Object> getHeader(Map<String, Object> map) {
        // to not introduce breaking-change we put default header values (e.g. method:'POST/PUT..')
        this.header.putAll(map);
        return this.header;
    }
}
