package apoc.export.util;

import apoc.meta.Meta;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ClassUtils;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResultTransformer;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static apoc.gephi.GephiFormatUtils.getCaption;
import static apoc.meta.Meta.Types.primitivesMapping;
import static apoc.meta.tablesforlabels.PropertyTracker.typeMappings;
import static java.util.Arrays.asList;
import static org.neo4j.internal.helpers.collection.Iterables.stream;

/**
 * @author mh
 * @since 19.01.14
 */
public class MetaInformation {
    
    private static final Map<String, String> REVERSED_TYPE_MAP = MapUtils.invertMap(typeMappings);
    
    public static Map<String, Class> collectPropTypesForNodes(SubGraph graph, GraphDatabaseService db, ExportConfig config) {
        if (!config.isSampling()) {
            Map<String,Class> propTypes = new LinkedHashMap<>();
            for (Node node : graph.getNodes()) {
                updateKeyTypes(propTypes, node);
            }
            return propTypes;
        }
        
        return db.executeTransactionally("CALL apoc.meta.nodeTypeProperties($conf)", 
                Map.of("conf", getConfWithIncludeLabels(graph, config)), getMapResultTransformer()); 
    }

    public static Map<String, Object> getConfWithIncludeLabels(SubGraph graph, ExportConfig config) {
        final Map<String, Object> conf = config.getSamplingConfig();
        conf.putIfAbsent("includeLabels", stream(graph.getAllLabelsInUse()).map(Label::name).collect(Collectors.toList()));
        return conf;
    }

    public static Map<String, Class> collectPropTypesForRelationships(SubGraph graph, GraphDatabaseService db, ExportConfig config) {
        if (!config.isSampling()) {
            Map<String,Class> propTypes = new LinkedHashMap<>();
            for (Relationship relationship : graph.getRelationships()) {
                updateKeyTypes(propTypes, relationship);
            }
            return propTypes;
        }
        
        return db.executeTransactionally("CALL apoc.meta.relTypeProperties($conf)", 
                Map.of("conf", getConfWithIncludeRels(graph, config)), getMapResultTransformer());
    }

    public static Map<String, Object> getConfWithIncludeRels(SubGraph graph, ExportConfig config) {
        final Map<String, Object> conf = config.getSamplingConfig();
        conf.putIfAbsent("includeRels", stream(graph.getAllRelationshipTypesInUse()).map(RelationshipType::name).collect(Collectors.toList()));
        return conf;
    }

    private static ResultTransformer<Map<String, Class>> getMapResultTransformer() {
        return result -> result.stream()
                .filter(map -> map.get("propertyName") != null)
                .collect(Collectors.toMap(map -> (String) map.get("propertyName"),
                        map -> {
                            final String propertyTypes = ((List<String>) map.get("propertyTypes")).get(0);
                            return getClassFromMetaType(propertyTypes);
                        }, (e1, e2) -> e2));
    }
    
    public static Class getClassAndConvertFromMeta(String propertyTypes) {
        final Class classFromMeta = getClassFromMetaType(propertyTypes);
        return convertPossiblyToPrimitive(classFromMeta);
    }

    public static void updateKeyTypes(Map<String, Class> keyTypes, Entity pc) {
        for (String prop : pc.getPropertyKeys()) {
            Object value = pc.getProperty(prop);
            Class storedClass = keyTypes.get(prop);
            if (storedClass==null) {
                keyTypes.put(prop,value.getClass());
                continue;
            }
            if (storedClass == void.class || storedClass.equals(value.getClass())) continue;
            keyTypes.put(prop, void.class);
        }
    }

    public static void updateKeyTypesForGraphMl(Map<String, Map<Class, String>> keyTypes, Entity pc, boolean useTypes) {
        for (String prop : pc.getPropertyKeys()) {
            Object value = pc.getProperty(prop);
            
            final Map<Class, String> propSubMap = keyTypes.get(prop);
            final Class classConverted = convertPossiblyToPrimitive(value.getClass());
            if (propSubMap == null) {
                putSubMap(keyTypes, prop, classConverted);
                continue;
            }
            propSubMap.putIfAbsent(classConverted, getPropSuffix(useTypes));
        }
    }

    public static void putSubMap(Map<String, Map<Class, String>> keyTypes, String prop, Class value) {
        keyTypes.put(prop, new HashMap<>(Map.of(value, StringUtils.EMPTY)));
    }

    public static String getPropSuffix(boolean useTypes) {
        return useTypes ? "_" + UUID.randomUUID().toString() : StringUtils.EMPTY;
    }

    public final static Set<String> GRAPHML_ALLOWED = new HashSet<>(asList("boolean", "int", "long", "float", "double", "string"));

    public static String typeFor(Class value, Set<String> allowed) {
        if (value == void.class) return null; // Is this necessary?
        Meta.Types type = Meta.Types.of(value);
        String name = (value.isArray() ? value.getComponentType() : value).getSimpleName().toLowerCase();
        boolean isAllowed = allowed != null && allowed.contains(name);
        switch (type) {
            case NULL:
                return null;
            case INTEGER: case FLOAT:
                return "integer".equals(name) || !isAllowed ? "int" : name;
            default:
                return isAllowed ? name : "string"; // We manage all other data types as strings
        }
    }

    public static String getLabelsString(Node node) {
        if (!node.getLabels().iterator().hasNext()) return "";
        String delimiter = ":";
        return delimiter + FormatUtils.joinLabels(node, delimiter);
    }

    public static String getLabelsStringGephi(ExportConfig config, Node node) {
        return getCaption(node, config.getCaption());
    }

    public static Class convertPossiblyToPrimitive(Class clazz) {
        // to avoid data type difference with apoc.meta.* and entity.getProperty(..).getClass() (e.g. Long[] and long[])
        if (clazz.isArray()) {
            // transform e.g. Long[] to long[] and long[] to long[]
            return Array.newInstance(convertPossiblyToPrimitive(clazz.getComponentType()), 0).getClass();
        }
        return primitivesMapping.getOrDefault(clazz, clazz);
    }

    private static Class getClassFromMetaType(String propertyTypes) {
        String className = REVERSED_TYPE_MAP.get(propertyTypes);
        try {
            return ClassUtils.getClass(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
