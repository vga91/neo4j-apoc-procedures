package apoc.export.cypher.formatter;

import apoc.export.cypher.TemplateCypher;
import apoc.export.util.ExportConfig;
import org.neo4j.graphdb.GraphDatabaseService;
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
public class UpdateAllCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {

	@Override
	public String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
	    return super.mergeStatementForNode(CypherFormat.UPDATE_ALL, node, uniqueConstraints, indexedProperties, indexNames);
	}

	@Override
	public String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties) {
        return super.mergeStatementForRelationship(CypherFormat.UPDATE_ALL, relationship, uniqueConstraints, indexedProperties);
	}

	@Override
	public void groupNodes(Iterable<Node> nodes, Map<String, Set<String>> uniqueConstraints, GraphDatabaseService db, TemplateCypher templateCypher) {
		super.groupNodes(nodes, uniqueConstraints, db, templateCypher);
	}

	@Override
	public void groupRelationships(Iterable<Relationship> relationships, Map<String, Set<String>> uniqueConstraints, GraphDatabaseService db, TemplateCypher templateCypher) {
		super.groupRelationships(relationships, uniqueConstraints, db, templateCypher);
	}

	@Override
	public void closeUnwindNodes(Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, Map.Entry<Set<String>, Set<String>> key, Node node) throws IOException {
		closeUnwindNodes("MERGE ", "SET ", uniqueConstraints, exportConfig, out, key, node);
	}

	@Override
	public void closeUnwindRelationships(Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, String start, String end, Map<String, Object> path, Relationship relationship) throws IOException {
		closeUnwindRelationships("MERGE ", "SET ", uniqueConstraints, exportConfig, out, start, end, path, relationship);
	}
}
