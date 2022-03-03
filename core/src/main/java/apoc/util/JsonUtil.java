package apoc.util;

import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 04.05.16
 */
public class JsonUtil {


    // public for test purpose
    public static final String KEY_ERROR = "errorList";

    public enum FailSilently { FALSE, WITH_LOG, WITH_LIST }
    
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

    // todo - forse non serve..
    private static Configuration getJsonPathConfig(List<String> options) {
        return getJsonPathConfig(options, OBJECT_MAPPER);
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
        return loadJson(url,headers,payload,"", true, null, null);
    }
    
    public static Stream<Object> loadJson(Object urlOrBinary, Map<String,Object> headers, String payload, String path, boolean failOnError, List<String> options) {
        return loadJson(urlOrBinary, headers, payload, path, failOnError, null, options);
    }
    
//    static class MappingIterator2<T> extends MappingIterator<T> {
//
//        protected MappingIterator2(JavaType type, JsonParser p, DeserializationContext ctxt, JsonDeserializer deser, boolean managedParser, Object valueToUpdate) {
//            super(type, p, ctxt, deser, managedParser, valueToUpdate);
//        }
//        
//        @Override
//        public T nextValue() throws IOException {
//            return super.nextValue();
//        }
//        
//        @Override
//        public T next()
//        {
//            return super.next();
//        }
//    }
    
    public static Stream<Object> loadJson(Object urlOrBinary, Map<String,Object> headers, String payload, String path, boolean failOnError, String compressionAlgo, List<String> options) {
        try {
            if (urlOrBinary instanceof String) {
                String url = (String) urlOrBinary;
                urlOrBinary = Util.getLoadUrlByConfigFile("json", url, "url").orElse(url);
            }
            InputStream input = FileUtils.inputStreamFor(urlOrBinary, headers, payload, compressionAlgo);
            JsonParser parser = OBJECT_MAPPER.getFactory().createParser(input);
            MappingIterator<Object> it = OBJECT_MAPPER.readValues(parser, Object.class);
            Stream<Object> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
            return StringUtils.isBlank(path) ? stream : stream.map((value) -> {
//                try {
                    return JsonPath.parse(value, getJsonPathConfig(options)).read(path);
//                } catch (Exception e) {
//                    System.out.println("exc = " + e);
//                    throw new RuntimeException(e);
//                }
            });
        } catch (IOException e) {
            if(!failOnError) {
                return Stream.of();
            } else {
                throw new RuntimeException(e);
            }
        }
//        catch (Exception e) {
//            System.out.println("JsonUtil.loadJson");
//            return null;
//        }
    }

    public static Stream<Object> loadJson(String url) {
        return loadJson(url,null,null,"", true, null, null);
    }

    public static <T> T parse(String json, String path, Class<T> type) {
        return parse(json, path, type, null);
    }

    public static <T> T parse(String json, String path, Class<T> type, List<String> options) {
        return parse(json, path, type, options, FailSilently.FALSE, null, false);
    }
    
    public static <T> T parse(String json, String path, Class<T> type, List<String> options, boolean validation) {
        return parse(json, path, type, options, FailSilently.WITH_LIST, null, validation);
    }
    

    public final static class SilentDeserializer extends UntypedObjectDeserializer {
        private final Log log;
        private final FailSilently failSilently;
        private final List<String> errorList = new ArrayList<>();
        
        public SilentDeserializer(FailSilently failSilently, Log log, JavaType listType, JavaType mapType) {
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
                final String errorKey = "__ERROR";
                switch (failSilently) {
                    case WITH_LIST:
                        errorList.add(errMsg);
                        return errorKey; // todo - key...
                    case WITH_LOG:
                        if (log != null) {
                            log.error(errMsg);
                        }
                        return errorKey;
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

//    public static class JacksonNonBlockingObjectMapperFactory {
//
//        /**
//         * Deserializer that won't block if value parsing doesn't match with target type
//         * @param <T> Handled type
//         */
//        private static class NonBlockingDeserializer<T> extends JsonDeserializer<T> {
//            private StdDeserializer<T> delegate;
//
//            public NonBlockingDeserializer(StdDeserializer<T> _delegate){
//                this.delegate = _delegate;
//            }
//
//            @Override
//            public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
//                try {
//                    return delegate.deserialize(jp, ctxt);
//                }catch (JsonMappingException e){
//                    // If a JSON Mapping occurs, simply returning null instead of blocking things
//                    return null;
//                }
//            }
//        }
//
//        private List<StdDeserializer> jsonDeserializers = new ArrayList<StdDeserializer>();
//
//        public ObjectMapper createObjectMapper(){
//            ObjectMapper objectMapper = new ObjectMapper();
//
//            SimpleModule customJacksonModule = new SimpleModule("customJacksonModule", new Version(1, 0, 0, null));
//            for(StdDeserializer jsonDeserializer : jsonDeserializers){
//                // Wrapping given deserializers with NonBlockingDeserializer
//                customJacksonModule.addDeserializer(jsonDeserializer.getValueClass(), new NonBlockingDeserializer(jsonDeserializer));
//            }
//
//            objectMapper.registerModule(customJacksonModule);
//            return objectMapper;
//        }
//
//        public JacksonNonBlockingObjectMapperFactory setJsonDeserializers(List<StdDeserializer> _jsonDeserializers){
//            this.jsonDeserializers = _jsonDeserializers;
//            return this;
//        }
//    }
    
    //
    //  TODO: vedere pr allow treat json as string
    //
    
//    static class JsonPath2 extends JsonPath {
//        
//        @Override
//        public Object parse(String json) throws InvalidJsonException {
//            try {
//                return objectReader.readValue(json);
//            } catch (IOException e) {
//                throw new InvalidJsonException(e, json);
//            }
//        }
//
//    }
    
    
    // TODO - farlo a tutti i json..., mettere opzione che fa tipo loadHtml - private enum FailSilently { FALSE, WITH_LOG, WITH_LIST }

    public static <T> T parse(String json, String path, Class<T> type, List<String> options, FailSilently failSilently, Log log) {
        return parse(json, path, type, options, failSilently, log, false);
    }
    
    public static <T> T parse(String json, String path, Class<T> type, List<String> options, FailSilently failSilently, Log log, boolean validation) {
        
        if (json==null || json.isEmpty()) return null;
        final String withListErrorMsg = String.format("The failSilently is set to %s but we don't know how to append the error message to type %s. So we just throw the parse message \n",
                FailSilently.WITH_LIST.name(), type);
        try {
            ObjectMapper objectMapper = OBJECT_MAPPER;
            final SilentDeserializer deser = new SilentDeserializer(failSilently, log, null, null);
            if (!failSilently.equals(FailSilently.FALSE)) {
//                SimpleModule module = new SimpleModule("Neo4jApocSerializer");
//                module.

                SimpleModule module = new SimpleModule("SilentDeserializer")
                        .addDeserializer(Object.class, deser);
                
                // todo - controllare che abbia anche PointSerializer e gli altri
                objectMapper = OBJECT_MAPPER.copy().registerModule(module);
//                        .setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
//                    @Override
//                    public Object findDeserializer(AnnotatedIntro a) {
//                        Object deserializer = super.findDeserializer(a);
//                        if (deserializer == null) {
//                            return null;
//                        }
//                        if (deserializer.equals(MyDeserializer.class)) {
//                            return null;
//                        }
//                        return deserializer;
//                    }
//                });
            }
            
            final String listOpt = Option.ALWAYS_RETURN_LIST.name();
            if (type == Map.class && options != null && options.contains(listOpt)) {
                throw new RuntimeException("It's not possible to use " + listOpt + " option because the conversion should return a Map");
            }
            if (path == null || path.isEmpty()) {
                final T t = (T) objectMapper.readValue(json, Object.class);
                return getJson(failSilently, withListErrorMsg, deser.getErrorList(), t, validation);
            }
            // https://stackoverflow.com/questions/9080904/jackson-deserialization-error-handling
            final DocumentContext parse = JsonPath.parse(json, getJsonPathConfig(options, objectMapper));
            final T t = parse.read(path, type);
            return getJson(failSilently, withListErrorMsg, deser.getErrorList(), t, validation);
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
//                    if (type.isAssignableFrom(Map.class) || type.equals(Object.class)) {
                        return (T) keyError;
                    } else if (type.isAssignableFrom(List.class)) {
                        return (T) List.of(keyError);
//                    } else if () {
                        
                    } else {
                        throw new RuntimeException(withListErrorMsg + errMessage, e);
                    }
                case FALSE:
                    throw new RuntimeException(errMessage, e);
            }
            if (failSilently.equals(FailSilently.FALSE)) {
                return null;
            }
            throw new RuntimeException(errMessage, e);
        } 
//        catch (Exception e) {
//            System.out.println("AAAAAAAAAAAAAa");
//            return null;
//        }
    }

    private static <T> T getJson(FailSilently failSilently, String withListErrorMsg, List<String> errorList, T json, boolean onlyValidation) {
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
                // todo - common...
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
