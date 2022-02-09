package apoc.load;

import java.util.Collections;
import java.util.Map;


public class CsvMapping extends BaseMapping {

    public CsvMapping(String name, LoadImportConfig config) {
        super(name, config);
        final Map<String, Object> mapping = (Map<String, Object>) config.getMapping().getOrDefault(name, Collections.emptyMap());
        
        if (this.type == null) {
            // Call this out to the user explicitly because deep inside of LoadCSV and others you will get
            // NPEs that are hard to spot if this is allowed to go through.
            throw new RuntimeException("In specified mapping, there is no type by the name " +
                    mapping.getOrDefault("type", "STRING").toString());
        }
    }
}