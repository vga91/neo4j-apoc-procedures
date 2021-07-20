package apoc.export.cypher.formatter;

import apoc.export.cypher.TemplateCypher;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportFormat;
import apoc.export.util.FormatUtils;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.export.util.FormatUtils.getLabelsSorted;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class CypherFormatterUtils {

    public final static String UNIQUE_ID_LABEL = "UNIQUE IMPORT LABEL";
    public final static String UNIQUE_ID_PROP = "UNIQUE IMPORT ID";
    public final static String Q_UNIQUE_ID_LABEL = quote(UNIQUE_ID_LABEL);

    public final static String FUNCTION_TEMPLATE = "%s('%s')";

    // ---- handlebars helpers ----

    public static void getGroupedNodes(Writer out, TemplateCypher templateCypher, Node node, Map<String, Set<String>> uniqueConstraints, Map.Entry<Set<String>, Set<String>> key, boolean isLast) throws IOException {
        ExportConfig exportConfig = templateCypher.getExportConfig();
        AtomicInteger unwindCount = templateCypher.getUnwindCount();
        AtomicInteger batchCount = templateCypher.getNodeBatchCount();
        final AtomicInteger propertiesCount = templateCypher.getPropertyCount();

        writeBatchBegin(exportConfig, out, batchCount);
        writeUnwindStart(exportConfig, out, unwindCount);
        batchCount.incrementAndGet();
        unwindCount.incrementAndGet();
        Map<String, Object> props = node.getAllProperties();
        // start element
        out.append("{");

        // id
        Map<String, Object> idMap = CypherFormatterUtils.getNodeIdProperties(node, uniqueConstraints);
        writeNodeIds(out, idMap);

        // properties
        out.append(", ");
        out.append("properties:");

        propertiesCount.addAndGet(props.size());
        props.keySet().removeAll(idMap.keySet());
        writeProperties(out, props);

        // end element
        out.append("}");
        if (isLast || isBatchMatch(exportConfig, batchCount) || isUnwindBatchMatch(exportConfig, unwindCount)) {
            exportConfig.getCypherFormat().getFormatter().closeUnwindNodes(uniqueConstraints, exportConfig, out, key, node);
            writeBatchEnd(exportConfig, out, batchCount);
            unwindCount.set(0);
        } else {
            out.append(", ");
        }
    }

    public static void getGroupedRels(Writer out, TemplateCypher templateCypher, Relationship rel, Map<String, Set<String>> uniqueConstraints, Map<String, Object> path, boolean isLast) throws IOException {
        ExportConfig exportConfig = templateCypher.getExportConfig();
        AtomicInteger unwindCount = templateCypher.getUnwindCount();
        AtomicInteger batchCount = templateCypher.getRelBatchCount();
        final AtomicInteger propertiesCount = templateCypher.getPropertyCount();

        String start = "start";
        String end = "end";

        writeBatchBegin(exportConfig, out, batchCount);
        writeUnwindStart(exportConfig, out, unwindCount);
        batchCount.incrementAndGet();
        unwindCount.incrementAndGet();
        Map<String, Object> props = rel.getAllProperties();
        // start element
        out.append("{");

        // start node
        Node startNode = rel.getStartNode();
        writeRelationshipNodeIds(uniqueConstraints, out, start, startNode);

        out.append(", ");

        // end node
        Node endNode = rel.getEndNode();
        writeRelationshipNodeIds(uniqueConstraints, out, end, endNode);

        // properties
        out.append(", ");
        out.append("properties:");
        writeProperties(out, props);
        propertiesCount.addAndGet(props.size());

        // end element
        out.append("}");

        if (isLast || isBatchMatch(exportConfig, batchCount) || isUnwindBatchMatch(exportConfig, unwindCount)) {
            exportConfig.getCypherFormat().getFormatter().closeUnwindRelationships(uniqueConstraints, exportConfig, out, start, end, path, rel);
            writeBatchEnd(exportConfig, out, batchCount);
            unwindCount.set(0);
        } else {
            out.append(", ");
        }
    }

    private static boolean isBatchMatch(ExportConfig exportConfig, AtomicInteger batchCount) {
        return batchCount.get() % exportConfig.getBatchSize() == 0;
    }

    private static void writeBatchBegin(ExportConfig exportConfig, Writer out, AtomicInteger batchCount) throws IOException {
        if (isBatchMatch(exportConfig, batchCount)) {
            out.append(exportConfig.getFormat().begin());
        }
    }

    private static void writeUnwindStart(ExportConfig exportConfig, Writer out, AtomicInteger batchCount) throws IOException {
        if (isUnwindBatchMatch(exportConfig, batchCount)) {
            String start = (exportConfig.getFormat() == ExportFormat.CYPHER_SHELL
                    && exportConfig.getOptimizationType() == ExportConfig.OptimizationType.UNWIND_BATCH_PARAMS) ?
                    ":param rows => [" : "UNWIND [";
            out.append(start);
        }
    }

    private static boolean isUnwindBatchMatch(ExportConfig exportConfig, AtomicInteger batchCount) {
        return batchCount.get() % exportConfig.getUnwindBatchSize() == 0;
    }


    private static void writeRelationshipNodeIds(Map<String, Set<String>> uniqueConstraints, Writer out, String key, Node node) throws IOException {
        String uniqueConstrainedLabel = getUniqueConstrainedLabel(node, uniqueConstraints);
        Set<String> props = getUniqueConstrainedProperties(uniqueConstraints, uniqueConstrainedLabel);
        Map<String, Object> properties;
        if (!props.contains(UNIQUE_ID_PROP)) {
            String[] propsArray = props.toArray(new String[props.size()]);
            properties = node.getProperties(propsArray);
        } else {
            // UNIQUE_ID_PROP is always the only member of the Set
            properties = Util.map(UNIQUE_ID_PROP, node.getId());
        }

        out.append(key + ": ");
        out.append("{");
        writeNodeIds(out, properties);
        out.append("}");
    }
    
    private static void writeBatchEnd(ExportConfig exportConfig, Writer out, AtomicInteger batchCount) throws IOException {
        if (isBatchMatch(exportConfig, batchCount)) {
            out.append(exportConfig.getFormat().commit());
        }
    }
    
    private static void writeProperties(Writer out, Map<String, Object> props) throws IOException {
        out.append("{");
        if (!props.isEmpty()) {
            int size = props.size();
            for (Map.Entry<String, Object> es : props.entrySet()) {
                --size;
                out.append(Util.quote(es.getKey()));
                out.append(":");
                out.append(CypherFormatterUtils.toString(es.getValue()));
                if (size > 0) {
                    out.append(", ");
                }
            }
        }
        out.append("}");
    }
    
    public static String getUniqueConstrainedLabel(Node node, Map<String, Set<String>> uniqueConstraints) {
        return uniqueConstraints.entrySet().stream()
                .filter(e -> node.hasLabel(Label.label(e.getKey())) && e.getValue().stream().anyMatch(k -> node.hasProperty(k)))
                .map(e -> e.getKey())
                .findFirst()
                .orElse(UNIQUE_ID_LABEL);
    }

    public static Set<String> getUniqueConstrainedProperties(Map<String, Set<String>> uniqueConstraints, String uniqueConstrainedLabel) {
        Set<String> props = uniqueConstraints.get(uniqueConstrainedLabel);
        if (props == null || props.isEmpty()) {
            props = Collections.singleton(UNIQUE_ID_PROP);
        }
        return props;
    }

    private static String formatNodeId(String key) {
        if (CypherFormatterUtils.UNIQUE_ID_PROP.equals(key)) {
            key = "_id";
        }
        return Util.quote(key);
    }

    private static void writeNodeIds(Writer out, Map<String, Object> properties) throws IOException {
        int size = properties.size();
        for (Map.Entry<String, Object> es : properties.entrySet()) {
            --size;
            out.append(formatNodeId(es.getKey()));
            out.append(":");
            out.append(CypherFormatterUtils.toString(es.getValue()));
            if (size > 0) {
                out.append(", ");
            }
        }
    }
    
    // ---- node id ----

    public static  String formatNodeLookup(String id, Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        result.append("(");
        result.append(id);
        result.append(getNodeIdLabels(node, uniqueConstraints, indexNames));
        Map<String, Object> nodeIdProperties = getNodeIdProperties(node, uniqueConstraints);
        if (nodeIdProperties.size() > 0) {
            result.append("{");
            StringBuilder props = new StringBuilder(100);
            for (String prop : nodeIdProperties.keySet()) {
                props.append(", ");
                props.append(quote(prop));
                props.append(":");
                props.append(CypherFormatterUtils.toString(nodeIdProperties.get(prop)));
            }
            result.append(props.substring(2));
            result.append("}");
        }
        result.append(")");
        return result.toString();
    }

    public static Map<String, Object> getNodeIdProperties(Node node, Map<String, Set<String>> uniqueConstraints) {
        Map<String, Object> nodeIdProperties = new LinkedHashMap<>();
        List<String> list = getLabelsSorted(node);

        for (String labelName : list) {
            if (!isUniqueLabelFound(node, uniqueConstraints, labelName)) {
                continue;
            }
            uniqueConstraints.get(labelName).forEach(prop -> {
                nodeIdProperties.put(prop, node.getProperty(prop));
            });
        }
        if (nodeIdProperties.isEmpty()) {
            nodeIdProperties.put(UNIQUE_ID_PROP, node.getId());
        }
        return nodeIdProperties;
    }

    // ---- labels ----

    public static String formatAllLabels(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        boolean uniqueLabelFound = false;
        List<String> list = getLabelsSorted(node);

        for (String labelName : list) {
            if (!uniqueLabelFound) {
                uniqueLabelFound = isUniqueLabelFound(node, uniqueConstraints, labelName);
            }
            if (indexNames != null && indexNames.contains(labelName))
                result.insert(0, label(labelName));
            else
                result.append(label(labelName));
        }
        if (!uniqueLabelFound) {
            result.append(label(UNIQUE_ID_LABEL));
        }
        return result.toString();
    }

    public static String formatNotUniqueLabels(String id, Node node, Map<String, Set<String>> uniqueConstraints) {
        StringBuilder result = new StringBuilder(100);
        List<String> list = getLabelsSorted(node);

        for (String labelName : list) {
            if (!isUniqueLabelFound(node, uniqueConstraints, labelName)) {
                result.append(", ");
                result.append(id);
                result.append(label(labelName));
            }
        }
        return formatToString(result);
    }

    private static String getNodeIdLabels(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        List<String> list = getLabelsSorted(node).stream()
                .filter(labelName -> isUniqueLabelFound(node, uniqueConstraints, labelName))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            result.append(label(UNIQUE_ID_LABEL));
        } else {
            list.forEach(labelName -> {
                if (indexNames != null && indexNames.contains(labelName)) {
                    result.insert(0, label(labelName));
                } else {
                    result.append(label(labelName));
                }
            });
        }
        return result.toString();
    }

    public static boolean isUniqueLabelFound(Node node, Map<String, Set<String>> uniqueConstraints, String labelName) {
        if (uniqueConstraints.containsKey(labelName)) {
            Set<String> nodeUniqueConstraint = uniqueConstraints.get(labelName);
            return nodeUniqueConstraint.stream().allMatch(node::hasProperty);
        } else {
            return false;
        }
    }

    // ---- properties ----

    public static String formatNodeProperties(String id, Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames, boolean jsonStyle) {
        StringBuilder result = formatProperties(id, node.getAllProperties(), jsonStyle);
        if (getNodeIdLabels(node, uniqueConstraints, indexNames).endsWith(label(UNIQUE_ID_LABEL))) {
            result.append(", ");
            result.append(formatPropertyName(id, UNIQUE_ID_PROP, node.getId(), jsonStyle));
        }
        return formatToString(result);
    }

    public static String formatRelationshipProperties(String id, Relationship relationship, boolean jsonStyle) {
        StringBuilder result = formatProperties(id, relationship.getAllProperties(), jsonStyle);
        return formatToString(result);
    }

    public static String formatNotUniqueProperties(String id, Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, boolean jsonStyle) {
        Map<String, Object> properties = new LinkedHashMap<>();
        List<String> keys = Iterables.asList(node.getPropertyKeys());
        Collections.sort(keys);
        Map<String, Object> nodeIdProperties = getNodeIdProperties(node, uniqueConstraints);
        for (String prop : keys) {
            if (!nodeIdProperties.containsKey(prop) && indexedProperties.contains(prop))
                properties.put(prop, node.getProperty(prop));
        }
        for (String prop : keys) {
            if (!nodeIdProperties.containsKey(prop) && !indexedProperties.contains(prop))
                properties.put(prop, node.getProperty(prop));
        }
        StringBuilder result = new StringBuilder(100);
        for (String key : properties.keySet()) {
            result.append(", ");
            result.append(formatPropertyName(id, key, properties.get(key), jsonStyle));
        }
        return formatToString(result);
    }

    public static String formatToString(StringBuilder result) {
        return result.length() > 0 ? result.substring(2) : "";
    }

    public static StringBuilder formatProperties(Map<String, Object> properties) {
        return formatProperties("", properties, true);
    }

    public static StringBuilder formatProperties(String id, Map<String, Object> properties, boolean jsonStyle) {
        StringBuilder result = new StringBuilder(100);
        if (properties != null) {
            List<String> keys = Iterables.asList(properties.keySet());
            Collections.sort(keys);
            for (String prop : keys) {
                result.append(", ");
                result.append(formatPropertyName(id, prop, properties.get(prop), jsonStyle));
            }
        }
        return result;
    }

    public static String formatPropertyName(String id, String prop, Object value, boolean jsonStyle) {
        return (id != null && !"".equals(id) ? id + "." : "") + quote(prop) + (jsonStyle ? ":" : "=" ) + toString(value);
    }

    // ---- to string ----

    public static String quote(Iterable<String> ids) {
        StringBuilder builder = new StringBuilder();
        for (Iterator<String> iterator = ids.iterator(); iterator.hasNext(); ) {
            String id = iterator.next();
            builder.append(quote(id));
            if (iterator.hasNext()) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    @Deprecated
    /**
     * use {@link Util#quote()}
     */
    public static String quote(String id) {
        return Util.quote(id);
    }

    public static String label(String id) {
        return ":" + quote(id);
    }

    public static String toString(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return FormatUtils.formatString(value);
        if (value instanceof Number) {
            return FormatUtils.formatNumber((Number) value);
        }
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Iterator) {
            return toString(((Iterator) value));
        }
        if (value instanceof Iterable) {
            return toString(((Iterable) value).iterator());
        }
        if (value.getClass().isArray()) {
            return arrayToString(value);
        }
        if (value instanceof Temporal){
            Value val = Values.of(value);
            return toStringFunction(val);
        }
        if (value instanceof DurationValue) {
            return toStringFunction((DurationValue) value);
        }
        return value.toString();
    }

    private static String toStringFunction(Value value) {
        return String.format(FUNCTION_TEMPLATE, value.getTypeName().toLowerCase(), value.toString());
    }

    public static String toString(Iterator<?> iterator) {
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            if (result.length() > 0) result.append(", ");
            Object value = iterator.next();
            result.append(toString(value));
        }
        return "[" + result + "]";
    }

    public static String arrayToString(Object value) {
        int length = Array.getLength(value);
        StringBuilder result = new StringBuilder(10 * length);
        for (int i = 0; i < length; i++) {
            if (i > 0) result.append(", ");
            result.append(toString(Array.get(value, i)));
        }
        return "[" + result.toString() + "]";
    }

    public static String cypherNode(Label label) {
        return String.format("(%s)", label == null ? "" : ":" + Util.quote(label.name()));
    }
}
