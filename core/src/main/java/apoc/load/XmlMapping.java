package apoc.load;

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

public class XmlMapping extends AbstractMapping {
    public XmlMapping(String name, LoadImportConfig config) {
        super(name, config);
    }

    public Object convert(Object value) {
        // in case of chars like '\n', with xml import for example
        if (value instanceof String && StringUtils.isBlank((String) value)) {
            return value;
        }
        return commonConvertType(value);
    }
    
    // todo - evaluate if might be worth using a convertArray() like CsvMapping
}