package apoc.util;

import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.NullValueProvider;
import com.fasterxml.jackson.databind.deser.ValueInstantiator;
import com.fasterxml.jackson.databind.deser.std.MapDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 04.05.16
 */
public class JsonUtil {
    private final static Option[] defaultJsonPathOptions = { Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS };
    
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
//    public static final ObjectMapper OBJECT_MAPPER;// = new ObjectMapper();
    public static final String PATH_OPTIONS_ERROR_MESSAGE = "Invalid pathOptions. The allowed values are: " + EnumSet.allOf(Option.class);
    static {
//        JacksonNonBlockingObjectMapperFactory factory = new JacksonNonBlockingObjectMapperFactory();
//        factory.setJsonDeserializers(Arrays.asList(new StdDeserializer[] {
////                // StdDeserializer, here, comes from Jackson (org.codehaus.jackson.map.deser.StdDeserializer)
////                new MapDeserializer(Map.class),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.IntegerDeserializer(Integer.class, null),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.CharacterDeserializer(Character.class, null),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.LongDeserializer(Long.class, null),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.FloatDeserializer(Float.class, null),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.DoubleDeserializer(Double.class, null),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.NumberDeserializer(),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.BigDecimalDeserializer(),
////                new org.codehaus.jackson.map.deser.std.StdDeserializer.BigIntegerDeserializer()
//////                new org.codehaus.jackson.map.deser.std.StdDeserializer.CalendarDeserializer()
//        }));
//        OBJECT_MAPPER = factory.createObjectMapper();
        
        
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        OBJECT_MAPPER.enable(DeserializationFeature.USE_LONG_FOR_INTS);
        
//        OBJECT_MAPPER.enable(DeserializationFeature.WRAP_EXCEPTIONS);
        
        
        SimpleModule module = new SimpleModule("Neo4jApocSerializer");
//        module.setDeserializerModifier(new BeanDeserializerModifier() {
//            @Override
//            public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) {
//                config.
//                return super.modifyDeserializer(config, beanDesc, deserializer);
//            }
//        })
        module.addSerializer(Point.class, new PointSerializer());
        module.addSerializer(Temporal.class, new TemporalSerializer());
        module.addSerializer(DurationValue.class, new DurationValueSerializer());
        
        
        
        
        module.addDeserializer(Object.class, new SilentDeserializer(null, null));
        
        
        
        
//        module.addDeserializer(Map.class, new SilentDeserializer2(null, null));
//        module.setDeserializerModifier()
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

    private static Configuration getJsonPathConfig(List<String> options) {
        try {
            Option[] opts = options == null ? defaultJsonPathOptions : options.stream().map(Option::valueOf).toArray(Option[]::new);
            return Configuration.builder()
                    .options(opts)
                    // ... questo
                    .jsonProvider(new JacksonJsonProvider(OBJECT_MAPPER))
                    .mappingProvider(new JacksonMappingProvider(OBJECT_MAPPER))
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

    public static class JacksonNonBlockingObjectMapperFactory {

        /**
         * Deserializer that won't block if value parsing doesn't match with target type
         * @param <T> Handled type
         */
        private static class NonBlockingDeserializer<T> extends JsonDeserializer<T> {
            private StdDeserializer<T> delegate;

            public NonBlockingDeserializer(StdDeserializer<T> _delegate){
                this.delegate = _delegate;
            }

            @Override
            public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                try {
                    return delegate.deserialize(jp, ctxt);
                }catch (JsonMappingException e){
                    // If a JSON Mapping occurs, simply returning null instead of blocking things
                    return null;
                }
            }
        }

        private List<StdDeserializer> jsonDeserializers = new ArrayList<StdDeserializer>();

        public ObjectMapper createObjectMapper(){
            ObjectMapper objectMapper = new ObjectMapper();

            SimpleModule customJacksonModule = new SimpleModule("customJacksonModule", new Version(1, 0, 0, null));
            for(StdDeserializer jsonDeserializer : jsonDeserializers){
                // Wrapping given deserializers with NonBlockingDeserializer
                customJacksonModule.addDeserializer(jsonDeserializer.getValueClass(), new NonBlockingDeserializer(jsonDeserializer));
            }

            objectMapper.registerModule(customJacksonModule);
            return objectMapper;
        }

        public JacksonNonBlockingObjectMapperFactory setJsonDeserializers(List<StdDeserializer> _jsonDeserializers){
            this.jsonDeserializers = _jsonDeserializers;
            return this;
        }
    }


    public static <T> T parse(String json, String path, Class<T> type, List<String> options) {
        return parse(json, path, type, options, false);
    }

    public static class SilentDeserializer2 extends MapDeserializer {

        public SilentDeserializer2(JavaType mapType, ValueInstantiator valueInstantiator, KeyDeserializer keyDeser, JsonDeserializer<Object> valueDeser, TypeDeserializer valueTypeDeser) {
            super(mapType, valueInstantiator, keyDeser, valueDeser, valueTypeDeser);
        }

        protected SilentDeserializer2(MapDeserializer src) {
            super(src);
        }

        protected SilentDeserializer2(MapDeserializer src, KeyDeserializer keyDeser, JsonDeserializer<Object> valueDeser, TypeDeserializer valueTypeDeser, NullValueProvider nuller, Set<String> ignorable) {
            super(src, keyDeser, valueDeser, valueTypeDeser, nuller, ignorable);
        }

        protected SilentDeserializer2(MapDeserializer src, KeyDeserializer keyDeser, JsonDeserializer<Object> valueDeser, TypeDeserializer valueTypeDeser, NullValueProvider nuller, Set<String> ignorable, Set<String> includable) {
            super(src, keyDeser, valueDeser, valueTypeDeser, nuller, ignorable, includable);
        }

        @Override
        public  Map<Object,Object>  deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            System.out.println("AAAAAA");
            return super.deserialize(jp, ctxt);
        }
    }

    // todo - ma questo richiama solo ObjectMapper, non 
    public final static class SilentDeserializer extends UntypedObjectDeserializer {

        public SilentDeserializer(JavaType listType, JavaType mapType) {
            super(listType, mapType);
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                // fallback to standard deserialization
                return super.deserialize(p, ctxt);
            } catch (Exception e) {
                System.out.println("CustomNumberSerializer.deserialize");
                return "porcoDio";
            }
        }

//        @Override
//        public Object readValue(JsonParser p, DeserializationContext ctxt) throws IOException {
//            try {
//                // fallback to standard deserialization
//                return super.deserialize(p, ctxt);
//            } catch (JsonMappingException e) {
//                System.out.println("CustomNumberSerializer.deserialize");
//                return "porcoDio";
//            }
//        }
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
    
    public static <T> T parse(String json, String path, Class<T> type, List<String> options, boolean failOnError) {
        if (json==null || json.isEmpty()) return null;
        try {
            final String listOpt = Option.ALWAYS_RETURN_LIST.name();
            if (type == Map.class && options != null && options.contains(listOpt)) {
                throw new RuntimeException("It's not possible to use " + listOpt + " option because the conversion should return a Map");
            }
            if (path == null || path.isEmpty()) {
                return (T) OBJECT_MAPPER.readValue(json, Object.class);
            }
            // https://stackoverflow.com/questions/9080904/jackson-deserialization-error-handling
            final DocumentContext parse = JsonPath.parse(json, getJsonPathConfig(options));
            return parse.read(path, type);
        } catch (IOException e) {
            if (!failOnError) {
                return null;
            }
            throw new RuntimeException("Can't convert " + json + " to "+type.getSimpleName()+" with path "+path, e);
        } catch (Exception e) {
            System.out.println("AAAAAAAAAAAAAa");
            return null;
        }
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
