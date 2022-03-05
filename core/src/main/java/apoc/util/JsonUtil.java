package apoc.util;

import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import apoc.load.util.ConversionUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.Temporal;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.load.util.ConversionUtil.KEY_ERROR;
import static apoc.load.util.ConversionUtil.FailSilently;
import static apoc.load.util.ConversionUtil.SilentDeserializer;

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

    static class NonClosingStream extends FilterInputStream {

        protected NonClosingStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
        }
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
            final FailSilently failSilently = failOnError ? FailSilently.FALSE : FailSilently.WITH_LOG;
            final SilentDeserializer deserializer = new SilentDeserializer(failSilently, log, null, null);
            ObjectMapper objectMapper = getObjectMapper(deserializer);
            JsonParser parser = objectMapper.getFactory().createParser(input);
            MappingIterator<Object> it = objectMapper.readValues(parser, Object.class);
            Stream<Object> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
            return StringUtils.isBlank(path) ? stream : stream.map((value) -> JsonPath.parse(value, getJsonPathConfig(options, objectMapper)).read(path));
        } catch (IOException e) {
            if(!failOnError) {
                return Stream.of();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static Stream<Object> loadJson(String url) {
        return loadJson(url,null,null,"", true, null, null, null);
    }

    public static <T> T parse(String json, String path, Class<T> type) {
        return parse(json, path, type, null);
    }

    public static <T> T parse(String json, String path, Class<T> type, List<String> options) {
        return parse(json, path, type, options, FailSilently.FALSE, null, false);
    }

    public static <T> T parse(String json, String path, Class<T> type, List<String> options, String failSilently, Log log) {
        final ConversionUtil.FailSilently failSilentlyEnum = ConversionUtil.FailSilently.valueOf(failSilently);
        return parse(json, path, type, options, failSilentlyEnum, log, false);
    }
    
    public static <T> T parse(String json, String path, Class<T> type, List<String> options, FailSilently failSilently, Log log, boolean validation) {
        
        if (json==null || json.isEmpty()) return null;
        final String withListErrorMsg = String.format("The failSilently is set to %s but we don't know how to append the error message to type %s. So we just throw the parse message \n",
                FailSilently.WITH_LIST.name(), type);
        try {
            SilentDeserializer deserializer = new SilentDeserializer(failSilently, log, null, null);
            ObjectMapper objectMapper = getObjectMapper(deserializer);
            final String listOpt = Option.ALWAYS_RETURN_LIST.name();
            if (type == Map.class && options != null && options.contains(listOpt)) {
                throw new RuntimeException("It's not possible to use " + listOpt + " option because the conversion should return a Map");
            }
            if (path == null || path.isEmpty()) {
                final T t = (T) objectMapper.readValue(json, Object.class);
                return getJson(failSilently, withListErrorMsg, deserializer, t, validation);
            }
            final T jsonParsed = JsonPath.parse(json, getJsonPathConfig(options, objectMapper)).read(path, type);
            return getJson(failSilently, withListErrorMsg, deserializer, jsonParsed, validation);
        } catch (Exception e) {
            final String errMessage = "Can't convert " + json + " to " + type.getSimpleName() + " with path " + path;
            switch (failSilently) {
                case WITH_LOG:
                    if (log != null) {
                        log.warn(errMessage);
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
            }
            throw new RuntimeException(errMessage, e);
        }
    }

    private static <T> T getJson(FailSilently failSilently, String withListErrorMsg, SilentDeserializer deserializer, T json, boolean onlyValidation) {
        List<String> errorList = deserializer.getErrorList();
        final Map<String, List<String>> keyError = Map.of(KEY_ERROR, errorList);
        if (onlyValidation) {
            return (T) errorList;
        }
        if (failSilently.equals(FailSilently.WITH_LIST)) {
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

    private static ObjectMapper getObjectMapper(SilentDeserializer deserializer) {
        SimpleModule module = new SimpleModule("SilentDeserializer")
                .addDeserializer(Object.class, deserializer);
        return OBJECT_MAPPER.copy().registerModule(module);
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
