package apoc.export.cypher.formatter;

import apoc.export.cypher.TemplateCypher;
import apoc.export.util.ExportConfig;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public interface CypherFormatter {

	String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames);

	String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties);

	String statementForIndex(String label, Iterable<String> keys, boolean ifNotExist);

	String statementForNodeFullTextIndex(String name, Iterable<Label> labels, Iterable<String> keys);

	String statementForRelationshipFullTextIndex(String name, Iterable<RelationshipType> types, Iterable<String> keys);

	String statementForConstraint(String label, Iterable<String> keys, boolean ifNotExist);

	String statementForCleanUp(int batchSize);

	void groupRelationships(Iterable<Relationship> relationships, Map<String, Set<String>> uniqueConstraints, GraphDatabaseService db, TemplateCypher templateCypher);
	
	void groupNodes(Iterable<Node> nodes, Map<String, Set<String>> uniqueConstraints, GraphDatabaseService db, TemplateCypher templateCypher);
	
	void closeUnwindNodes(Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, Map.Entry<Set<String>, Set<String>> key, Node node) throws IOException;

	void closeUnwindRelationships(Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, String start, String end, Map<String, Object> path, Relationship rel) throws IOException;

}
