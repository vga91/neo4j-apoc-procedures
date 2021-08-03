package apoc.load;

import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.CompressionConfig.COMPRESSION;

public class LoadJson {

    private static final String AUTH_HEADER_KEY = "Authorization";
    private static final String LOAD_TYPE = "json";

    @Context
    public GraphDatabaseService db;

    @SuppressWarnings("unchecked")
    @Procedure
    @Description("apoc.load.jsonArray('url') YIELD value - load array from JSON URL (e.g. web-api) to import JSON as stream of values")
    public Stream<ObjectResult> jsonArray(@Name("url") String url, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return JsonUtil.loadJson(url, null, null, path, true, (List<String>) config.get("pathOptions"))
                .flatMap((value) -> {
                    if (value instanceof List) {
                        List list = (List) value;
                        if (list.isEmpty()) return Stream.empty();
                        if (list.get(0) instanceof Map) return list.stream().map(ObjectResult::new);
                    }
                    return Stream.of(new ObjectResult(value));
                });
        // throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
    }

    @Procedure
    @Description("apoc.load.json('urlOrKeyOrBinary',path, config) YIELD value - import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> json(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return jsonParams(urlOrKeyOrBinary,null,null, path, config);
    }

    @SuppressWarnings("unchecked")
    @Procedure
    @Description("apoc.load.jsonParams('urlOrKeyOrBinary',{header:value},payload, config) YIELD value - load from JSON URL (e.g. web-api) while sending headers / payload to import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> jsonParams(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name("headers") Map<String,Object> headers, @Name("payload") String payload, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        return loadJsonStream(urlOrKeyOrBinary, headers, payload, path, config);
    }

    public static Stream<MapResult> loadJsonStream(@Name("url") Object url, @Name("headers") Map<String, Object> headers, @Name("payload") String payload) {
        return loadJsonStream(url, headers, payload, "", Collections.emptyMap());
    }
    
    public static Stream<MapResult> loadJsonStream(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name("headers") Map<String, Object> headers, @Name("payload") String payload, String path, Map<String, Object> config) {
        LoadJsonConfig jsonConfig = new LoadJsonConfig(config);
        boolean failOnError = jsonConfig.isFailOnError();
        String compressionAlgo = jsonConfig.getCompressionAlgo();
        List<String> pathOptions = jsonConfig.getPathOptions();

        if (urlOrKeyOrBinary instanceof String) {
            headers = null != headers ? headers : new HashMap<>();
            headers.putAll(Util.extractCredentialsIfNeeded((String) urlOrKeyOrBinary, failOnError));
        }
        Stream<Object> stream = JsonUtil.loadJson(urlOrKeyOrBinary,headers,payload, path, failOnError, compressionAlgo, pathOptions);
        return stream.flatMap((value) -> {
            if (value instanceof Map) {
                return Stream.of(new MapResult(convertTypeMap((Map) value, jsonConfig)));
            }
            if (value instanceof List) {
                if (((List)value).isEmpty()) return Stream.empty();
                if (((List) value).get(0) instanceof Map)
                    return ((List) value).stream().map((v) -> new MapResult(convertTypeMap((Map) v, jsonConfig)));
                return Stream.of(new MapResult(convertTypeMap(Collections.singletonMap("result",value), jsonConfig)));
            }
            if(!failOnError)
                throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
            else
                return Stream.of(new MapResult(Collections.emptyMap()));
        });
    }

    private static Map<String, Object> convertTypeMap(Map<String, Object> mapValue, LoadJsonConfig config) {
        return mapValue.entrySet()
                .stream()
                .collect(HashMap::new,
                        (mapAccumulator, entry) -> {
                            final Map<String, Map<String, Object>> mapping = config.getMapping();
                            final String key = entry.getKey();
                            final Object value = entry.getValue();
                            mapAccumulator.put(key,
                                    mapping.containsKey(key) 
                                            ? new JsonMapping(key, mapping.get(key), config.getIgnore().contains(key), config.getNullValues(), config.getZoneId()).convert(value) 
                                            : (value instanceof Map ? convertTypeMap((Map) value, config) : value)
                            );
                        },
                        HashMap::putAll);
    }

    // todo - forse è meglio come sottoclasse? o come classe a parte
    public static class JsonMapping extends AbstractMapping {

        public JsonMapping(String name, Map<String, Object> mapping, boolean ignore, List<String> nullValues, ZoneId timezone) {
            super(name, mapping, ignore, nullValues, timezone);
        }

        public Object convert(Object value) {
            return value instanceof List ? convertList((List) value) : switchConvertType(value);
        }

        private Object convertList(List<Object> value) {
            return value.stream().map(super::switchConvertType).collect(Collectors.toList());
        }
    }
    
}
