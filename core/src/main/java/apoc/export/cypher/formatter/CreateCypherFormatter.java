package apoc.export.cypher.formatter;

import apoc.export.util.ExportConfig;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class CreateCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {


    @Override
    public String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        result.append("CREATE (");
        String labels = CypherFormatterUtils.formatAllLabels(node, uniqueConstraints, indexNames);
        if (!labels.isEmpty()) {
            result.append(labels);
        }
        if (node.getPropertyKeys().iterator().hasNext()) {
            result.append(" {");
            result.append(CypherFormatterUtils.formatNodeProperties("", node, uniqueConstraints, indexNames, true));
            result.append("}");
        }
        result.append(");" + StringUtils.LF);
        return result.toString();
    }

    @Override
    public String statementForRelationship(Relationship relationship,  Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties) {
        StringBuilder result = new StringBuilder(100);
        result.append("MATCH ");
        result.append(CypherFormatterUtils.formatNodeLookup("n1", relationship.getStartNode(), uniqueConstraints, indexedProperties));
        result.append(", ");
        result.append(CypherFormatterUtils.formatNodeLookup("n2", relationship.getEndNode(), uniqueConstraints, indexedProperties));
        result.append(" CREATE (n1)-[r:" + CypherFormatterUtils.quote(relationship.getType().name()));
        if (relationship.getPropertyKeys().iterator().hasNext()) {
            result.append(" {");
            result.append(CypherFormatterUtils.formatRelationshipProperties("", relationship, true));
            result.append("}");
        }
        result.append("]->(n2);" + StringUtils.LF);
        return result.toString();
    }

    @Override
    public void closeUnwindNodes(Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, Map.Entry<Set<String>, Set<String>> key, Node node) throws IOException {
        closeUnwindNodes("CREATE ", "SET ", uniqueConstraints, exportConfig, out, key, node);
    }

    @Override
    public void closeUnwindRelationships(Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, String start, String end, Map<String, Object> path, Relationship relationship) throws IOException {
        closeUnwindRelationships("CREATE ", "SET ", uniqueConstraints, exportConfig, out, start, end, path, relationship);
    }

}