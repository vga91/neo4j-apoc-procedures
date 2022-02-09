package apoc.load;

import org.apache.commons.lang3.StringUtils;

public class XmlMapping extends BaseMapping {
    public XmlMapping(String name, LoadImportConfig config) {
        super(name, config);
    }

    @Override
    public Object convert(Object value) {
        // in case of chars like '\n', with xml import for example
        if (value instanceof String && StringUtils.isBlank((String) value)) {
            return value;
        }
        return super.convert(value);
    }
}