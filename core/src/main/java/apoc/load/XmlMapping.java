package apoc.load;

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;

public class XmlMapping extends AbstractMapping {
    public XmlMapping(String name, Map<String, Object> mapping, boolean ignore, Collection<String> nullValues, ZoneId zoneId) {
        super(name, mapping, ignore, nullValues, zoneId);
    }

    public Object convert(Object value) {
        // in case of chars like '\n', with xml import for example
        if (value instanceof String && StringUtils.isBlank((String) value)) {
            return value;
        }
        return commonConvertType(value);
    }
}