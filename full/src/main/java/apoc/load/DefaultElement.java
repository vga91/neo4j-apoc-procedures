package apoc.load;

import org.apache.commons.collections.MapUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.neo4j.logging.Log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static apoc.load.LoadHtml.getElements;
import static apoc.meta.Meta.Types.primitivesMapping;

public class DefaultElement implements LoadHtmlFunctions.LoadElementInterface {
    private final Object[] params;
    private final String name;

    public DefaultElement(Object[] params, String name) {
        this.params = params;
        this.name = name;
    }

    @Override
    public List<Map<String, Object>> get(Document document, Map<String, Object> config, List<String> errorList, Log log) {
        final Map<Class<?>, Class<?>> mapWrapperToPrimitive = MapUtils.invertMap(primitivesMapping);
        final Class[] types = Arrays.stream(params).map(Object::getClass)
                .map(i -> mapWrapperToPrimitive.getOrDefault(i, i))
                .toArray(Class[]::new);
        try {
            final Method method = Element.class.getMethod(name, types);
            final Object invoke = method.invoke(document, params);
            final Elements select = invoke instanceof Element ? new Elements((Element) invoke) : (Elements) invoke;
            return getElements(select, config, errorList, log);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
