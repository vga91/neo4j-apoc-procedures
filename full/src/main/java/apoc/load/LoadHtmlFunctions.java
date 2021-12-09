package apoc.load;

import org.jsoup.nodes.Document;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;

public class LoadHtmlFunctions {
    
    public static LoadElementInterface from(Object value) {
        if (value instanceof List) {
            final List value2 = (List) value;
            String value1 = (String) value2.get(0);
            final Object[] params = value2.stream().skip(1).toArray();
            return getLoadElementInterface(value1, params);
        }
        if (value instanceof String) {
            return getLoadElementInterface((String) value, new Object[0]);
        }
        throw new RuntimeException("Value parameter must be string or list");
    }

    private static LoadElementInterface getLoadElementInterface(String funName, Object[] params) {
        if (funName.endsWith("()")) {
            funName = funName.substring(0, funName.length() - 2);
            switch (funName) {
                case "getLinks":
                    return new SelectElement("a[href]");
                case "getMediaLinks":
                    return new SelectElement("[src]");
                case "getPlainText":
                    return new PlainText(params.length == 0 ? null : (String) params[0]);
                default:
                    return new DefaultElement(params, funName);
            }
        } else {
            return new SelectElement(funName);
        }
    }
    
    
    public interface LoadElementInterface {
        Object get(Document document, Map<String, Object> config, List<String> errorList, Log log);
    }
    
}
