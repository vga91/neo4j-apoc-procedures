package apoc.export.arrow;

import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class ImportArrowConfig {

    private final Map<String, Map<String, String>> nodePropertyMappings;
    private final Map<String, Map<String, String>> relPropertyMappings;

    private final int batchSize;

    public ImportArrowConfig(Map<String, Object> config) {
        config = config == null ? Collections.emptyMap() : config;
        this.nodePropertyMappings = (Map<String, Map<String, String>>) config.getOrDefault("nodePropertyMappings", Collections.emptyMap());
        this.relPropertyMappings = (Map<String, Map<String, String>>) config.getOrDefault("relPropertyMappings", Collections.emptyMap());
        this.batchSize = Util.toInteger(config.getOrDefault("batchSize", 2000));
    }

    public String typeForNode(Collection<String> labels, String property) {
        return labels.stream()
                .map(label -> nodePropertyMappings.getOrDefault(label, Collections.emptyMap()).get(property))
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
    }

    public String typeForRel(String type, String property) {
        return relPropertyMappings.getOrDefault(type, Collections.emptyMap()).get(property);
    }

    public int getBatchSize() {
        return batchSize;
    }
}
