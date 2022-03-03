package apoc.util;

import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.DurationValue;

import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.ConversionUtil.ERROR_VALUE;
import static apoc.util.ConversionUtil.KEY_ERROR;

/**
 * @author mh
 * @since 04.05.16
 */
public class JsonUtil {
    
    private final static Option[] defaultJsonPathOptions = { Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS };
    
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String PATH_OPTIONS_ERROR_MESSAGE = "Invalid pathOptions. The allowed values are: " + EnumSet.allOf(Option.class);
    static {
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        OBJECT_MAPPER.enable(DeserializationFeature.USE_LONG_FOR_INTS);
        SimpleModule module = new SimpleModule("Neo4jApocSerializer");
        module.addSerializer(Point.class, new PointSerializer());
        module.addSerializer(Temporal.class, new TemporalSerializer());
        module.addSerializer(DurationValue.class, new DurationValueSerializer());
        OBJECT_MAPPER.registerModule(module);
    }

    private static Configuration getJsonPathConfig(List<String> options, ObjectMapper objectMapper) {
        try {
            Option[] opts = options == null ? defaultJsonPathOptions : options.stream().map(Option::valueOf).toArray(Option[]::new);
            return Configuration.builder()
                    .options(opts)
                    .jsonProvider(new JacksonJsonProvider(objectMapper))
                    .mappingProvider(new JacksonMappingProvider(objectMapper))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(PATH_OPTIONS_ERROR_MESSAGE, e);
        }
    }
    
    public static Stream<Object> loadJson(String url, Map<String,Object> headers, String payload) {
        return loadJson(url,headers,payload,"", true, null, null, null);
    }
    
    public static Stream<Object> loadJson(Object urlOrBinary, Map<String,Object> headers, String payload, String path, boolean failOnError, List<String> options) {
        return loadJson(urlOrBinary, headers, payload, path, failOnError, null, options, null);
    }
    
    public static Stream<Object> loadJson(Object urlOrBinary, Map<String,Object> headers, String payload, String path, boolean failOnError, String compressionAlgo, List<String> options, Log log) {
        try {
            if (urlOrBinary instanceof String) {
                String url = (String) urlOrBinary;
                urlOrBinary = Util.getLoadUrlByConfigFile("json", url, "url").orElse(url);
            }
            InputStream input = FileUtils.inputStreamFor(urlOrBinary, headers, payload, compressionAlgo);
            final ConversionUtil.FailSilently failSilently = failOnError ? ConversionUtil.FailSilently.FALSE : ConversionUtil.FailSilently.WITH_LOG;
            final SilentDeserializer deser = new SilentDeserializer(failSilently, log, null, null);
            ObjectMapper objectMapper = getObjectMapper(failSilently, deser);
            JsonParser parser = objectMapper.getFactory().createParser(input);
            MappingIterator<Object> it = objectMapper.readValues(parser, Object.class);
            Stream<Object> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
            return StringUtils.isBlank(path) ? stream : stream.map((value) -> {

                    return JsonPath.parse(value, getJsonPathConfig(options, objectMapper)).read(path);
            });
        } catch (IOException e) {
            if(!failOnError) {
                return Stream.of();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static ObjectMapper getObjectMapper(ConversionUtil.FailSilently failSilently, SilentDeserializer deser) {
        if (!failSilently.equals(ConversionUtil.FailSilently.FALSE)) {
            SimpleModule module = new SimpleModule("SilentDeserializer")
                    .addDeserializer(Object.class, deser);

            return OBJECT_MAPPER.copy().registerModule(module);
        }
        return OBJECT_MAPPER;
    }

    public static Stream<Object> loadJson(String url) {
        return loadJson(url,null,null,"", true, null, null, null);
    }

    public static <T> T parse(String json, String path, Class<T> type) {
        return parse(json, path, type, null);
    }

    public static <T> T parse(String json, String path, Class<T> type, List<String> options) {
        return parse(json, path, type, options, ConversionUtil.FailSilently.FALSE, null, false);
    }
    
    public static <T> T parse(String json, String path, Class<T> type, List<String> options, boolean validation) {
        return parse(json, path, type, options, ConversionUtil.FailSilently.WITH_LIST, null, validation);
    }
    

    public final static class SilentDeserializer extends UntypedObjectDeserializer {
        private final Log log;
        private final ConversionUtil.FailSilently failSilently;
        private final List<String> errorList = new ArrayList<>();
        
        public SilentDeserializer(ConversionUtil.FailSilently failSilently, Log log, JavaType listType, JavaType mapType) {
            super(listType, mapType);
            this.log = log;
            this.failSilently = failSilently;
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                // fallback to standard deserialization
                return super.deserialize(p, ctxt);
            } catch (IOException e) {
                final String errMsg = "Error with key " + p.getParsingContext().getCurrentName() + " - " + e.getMessage();
                switch (failSilently) {
                    case WITH_LIST:
                        errorList.add(errMsg);
                        return ERROR_VALUE;
                    case WITH_LOG:
                        if (log != null) {
                            log.error(errMsg);
                        }
                        return ERROR_VALUE;
                    default:
                        throw new IOException(e);
                }
            } catch (Exception e) {
                System.out.println("qui non dovrebbe mai andarci... credo...");
                return null;
            }
        }

        public List<String> getErrorList() {
            return errorList;
        }
    }

    public static <T> T parse(String json, String path, Class<T> type, List<String> options, ConversionUtil.FailSilently failSilently, Log log) {
        return parse(json, path, type, options, failSilently, log, false);
    }
    
    public static <T> T parse(String json, String path, Class<T> type, List<String> options, ConversionUtil.FailSilently failSilently, Log log, boolean validation) {
        
        if (json==null || json.isEmpty()) return null;
        final String withListErrorMsg = String.format("The failSilently is set to %s but we don't know how to append the error message to type %s. So we just throw the parse message \n",
                ConversionUtil.FailSilently.WITH_LIST.name(), type);
        try {
            SilentDeserializer deserializer = new SilentDeserializer(failSilently, log, null, null);
            ObjectMapper objectMapper = getObjectMapper(failSilently, deserializer);
            final String listOpt = Option.ALWAYS_RETURN_LIST.name();
            if (type == Map.class && options != null && options.contains(listOpt)) {
                throw new RuntimeException("It's not possible to use " + listOpt + " option because the conversion should return a Map");
            }
            if (path == null || path.isEmpty()) {
                final T t = (T) objectMapper.readValue(json, Object.class);
                return getJson(failSilently, withListErrorMsg, deserializer, t, validation);
            }
            final T t = JsonPath.parse(json, getJsonPathConfig(options, objectMapper)).read(path, type);
            return getJson(failSilently, withListErrorMsg, deserializer, t, validation);
        } catch (Exception e) {
            final String errMessage = "Can't convert " + json + " to " + type.getSimpleName() + " with path " + path;
            switch (failSilently) {
                case WITH_LOG:
                    if (log != null) {
                        log.error(errMessage);
                    }
                    break;
                case WITH_LIST:
                    final Map<String, String> keyError = Map.of(KEY_ERROR, errMessage);
                    if (type.isAssignableFrom(Map.class) || type.equals(Object.class)) {
                        return (T) keyError;
                    } else if (type.isAssignableFrom(List.class)) {
                        return (T) List.of(keyError);
                    } else {
                        throw new RuntimeException(withListErrorMsg + errMessage, e);
                    }
                case FALSE:
                    throw new RuntimeException(errMessage, e);
            }
            if (failSilently.equals(ConversionUtil.FailSilently.FALSE)) {
                return null;
            }
            throw new RuntimeException(errMessage, e);
        }
    }

    private static <T> T getJson(ConversionUtil.FailSilently failSilently, String withListErrorMsg, SilentDeserializer deserializer, T json, boolean onlyValidation) {
        List<String> errorList = deserializer.getErrorList();
        final Map<String, List<String>> keyError = Map.of(KEY_ERROR, errorList);
        if (onlyValidation) {
            return (T) errorList;
        }
        if (failSilently.equals(ConversionUtil.FailSilently.WITH_LIST)) {
            if (json instanceof Map) {
                ((Map) json).putAll(keyError);
            } else if (json instanceof List) {
                ((List) json).add(keyError);
            } else {
                throw new RuntimeException(withListErrorMsg + String.join(", ", errorList));
            }
        }
        return json;
    }

    public static String writeValueAsString(Object json) {
        try {
            return OBJECT_MAPPER.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static byte[] writeValueAsBytes(Object json) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(json);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
