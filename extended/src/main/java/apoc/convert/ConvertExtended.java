package apoc.convert;

import apoc.Extended;
import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import apoc.meta.Types;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;

import java.io.IOException;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

@Extended
public class ConvertExtended {
    
    
    @UserFunction("apoc.convert.toYaml")
    @Description("Serializes the given YAML value.")
    public String toYaml(@Name("value") Object value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try {
            Object result = writeYamlResult(value);

            List<String> enable = (List<String>) config.getOrDefault("enable", List.of());
            List<String> disable = (List<String>) config.getOrDefault("disable", List.of());

            YAMLFactory factory = new YAMLFactory();
            
            enable.forEach( name -> factory.enable(YAMLGenerator.Feature.valueOf(name)) );
            disable.forEach( name -> factory.disable(YAMLGenerator.Feature.valueOf(name)) );

            SimpleModule module = new SimpleModule("Neo4jApocSerializer");
            module.addSerializer(Point.class, new PointSerializer());
            module.addSerializer(Temporal.class, new TemporalSerializer());
            module.addSerializer(DurationValue.class, new DurationValueSerializer());

            ObjectMapper objectMapper = new ObjectMapper(factory);
            
            objectMapper.registerModule(module);
            return objectMapper.writeValueAsString(result);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to yaml", e);
        }
    }

    // todo - Excended util??
    public static Object writeYamlResult(Object value) {
        Types type = Types.of(value);
        switch (type) {
            case NODE:
                return nodeToMap((Node) value);
            case RELATIONSHIP:
                return relToMap((Relationship) value);
            case PATH:
                return writeYamlResult(StreamSupport.stream(((Path) value).spliterator(), false)
                        .map(i -> i instanceof Node ? nodeToMap((Node) i) : relToMap((Relationship) i))
                        .collect(Collectors.toList()));
            case LIST:
                return ConvertUtils.convertToList(value).stream()
                        .map(ConvertExtended::writeYamlResult)
                        .collect(Collectors.toList());
            case MAP:
                return ((Map<String, Object>) value)
                        .entrySet().stream()
                        .collect(
                                HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                                (mapAccumulator, entry) ->
                                        mapAccumulator.put(entry.getKey(), writeYamlResult(entry.getValue())),
                                HashMap::putAll);
            default:
                return value;
        }
    }

    // visible for testing
    public static String NODE = "node";
    public static String RELATIONSHIP = "relationship";

    private static Map<String, Object> relToMap(Relationship rel) {
        Map<String, Object> mapRel = map(
                "id", rel.getElementId(),
                "type", RELATIONSHIP,
                "label", rel.getType().toString(),
                "start", nodeToMap(rel.getStartNode()),
                "end", nodeToMap(rel.getEndNode()));

        return mapWithOptionalProps(mapRel, rel.getAllProperties());
    }

    private static Map<String, Object> nodeToMap(Node node) {
        Map<String, Object> mapNode = map("id", node.getElementId());

        mapNode.put("type", NODE);

        if (node.getLabels().iterator().hasNext()) {
            mapNode.put("labels", labelStrings(node));
        }
        return mapWithOptionalProps(mapNode, node.getAllProperties());
    }

    private static Map<String, Object> mapWithOptionalProps(Map<String, Object> mapEntity, Map<String, Object> props) {
        if (!props.isEmpty()) {
            mapEntity.put("properties", props);
        }
        return mapEntity;
    }
}
