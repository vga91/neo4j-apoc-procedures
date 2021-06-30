package apoc.export.util;

import apoc.convert.Convert;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

/**
 * @author mh
 * @since 23.02.16
 */
public class FormatUtils {

    public static String formatNumber(Number value) {
        if (value == null) return null;
        return value.toString();
    }

    public static String formatString(Object value) {
        return "\"" + String.valueOf(value).replaceAll("\\\\", "\\\\\\\\")
                .replaceAll("\n","\\\\n")
                .replaceAll("\r","\\\\r")
                .replaceAll("\t","\\\\t")
                .replaceAll("\"","\\\\\"") + "\"";
    }

    public static String joinLabels(Node node, String delimiter) {
        return getLabelsAsStream(node).collect(Collectors.joining(delimiter));
    }

    public static Map<String,Object> toMap(Entity pc) {
        if (pc == null) return null;
        if (pc instanceof Node) {
            Node node = (Node) pc;
            return map("id", node.getId(), "labels",labelStrings(node),
                    "properties",pc.getAllProperties());
        }
        if (pc instanceof Relationship) {
            Relationship rel = (Relationship) pc;
            return map("id", rel.getId(), "type", rel.getType().name(),
                    "start", rel.getStartNode().getId(),"end", rel.getEndNode().getId(),
                    "properties",pc.getAllProperties());
        }
        throw new RuntimeException("Invalid graph element "+pc);
    }

    public static String toString(Object value, boolean importToolArrays, String arrayDelimiter) {
        if (value == null) return "";
        if (value instanceof Path) {
            return toString(StreamSupport.stream(((Path)value).spliterator(),false).map(FormatUtils::toMap).collect(Collectors.toList()));
        }
        if (value instanceof Entity) {
            return Util.toJson(toMap((Entity) value)); // todo id, label, type ?
        }
        boolean isArray = value.getClass().isArray();
        if (isArray || value instanceof Iterable) {
            if (importToolArrays) {
                Stream stream = isArray
                        ? Arrays.stream(Convert.getObjects(value))
                        : StreamSupport.stream(((Iterable) value).spliterator(), false);

                return (String) stream.map(item -> toString(item, true, arrayDelimiter))
                        .collect(Collectors.joining(arrayDelimiter));
            }
            return Util.toJson(value);
        }
        if (value instanceof Map) {
            return Util.toJson(value);
        }
        if (value instanceof Number) {
            return formatNumber((Number)value);
        }
        if (value instanceof Point) {
            return formatPoint((Point) value);
        }
        return value.toString();
    }

    public static String toString(Object value) {
        return toString(value, false, null);
    }

    public static String formatPoint(Point value) {
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getLabelsSorted(Node node) {
        return getLabelsAsStream(node).collect(Collectors.toList());
    }

    private static Stream<String> getLabelsAsStream(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(),false).map(Label::name).sorted();
    }
}
