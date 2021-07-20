package apoc.export.cypher.formatter;

import apoc.export.cypher.TemplateCypher;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportFormat;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.export.cypher.formatter.CypherFormatterUtils.Q_UNIQUE_ID_LABEL;
import static apoc.export.cypher.formatter.CypherFormatterUtils.UNIQUE_ID_PROP;
import static apoc.export.cypher.formatter.CypherFormatterUtils.getUniqueConstrainedLabel;
import static apoc.export.cypher.formatter.CypherFormatterUtils.getUniqueConstrainedProperties;
import static apoc.export.cypher.formatter.CypherFormatterUtils.quote;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
abstract class AbstractCypherFormatter implements CypherFormatter {

	private static final String STATEMENT_CONSTRAINTS = "CREATE CONSTRAINT%s ON (node:%s) ASSERT (%s) %s;" + StringUtils.LF;

	private static final String STATEMENT_NODE_FULLTEXT_IDX = "CALL db.index.fulltext.createNodeIndex('%s',[%s],[%s]);" + StringUtils.LF;
	private static final String STATEMENT_REL_FULLTEXT_IDX = "CALL db.index.fulltext.createRelationshipIndex('%s',[%s],[%s]);" + StringUtils.LF;
	public static final String PROPERTY_QUOTING_FORMAT = "'%s'";

	@Override
	public String statementForCleanUp(int batchSize) {
		return "MATCH (n:" + Q_UNIQUE_ID_LABEL + ") " +
				" WITH n LIMIT " + batchSize +
				" REMOVE n:" + Q_UNIQUE_ID_LABEL + " REMOVE n." + quote(UNIQUE_ID_PROP) + ";" + StringUtils.LF;
	}

	@Override
	public String statementForIndex(String label, Iterable<String> keys, boolean ifNotExists) {
		return String.format("CREATE INDEX%s FOR (node:%s) ON (%s);" + StringUtils.LF;, 
				getIfNotExists(ifNotExists),
				Util.quote(label),
				getPropertiesQuoted(keys));
	}

	@Override
	public String statementForNodeFullTextIndex(String name, Iterable<Label> labels, Iterable<String> keys) {
		String label = StreamSupport.stream(labels.spliterator(), false)
				.map(Label::name)
				.map(Util::quote)
				.map(s -> String.format(PROPERTY_QUOTING_FORMAT, s))
				.collect(Collectors.joining(","));
		String key = StreamSupport.stream(keys.spliterator(), false)
				.map(Util::quote)
				.map(s -> String.format(PROPERTY_QUOTING_FORMAT, s))
				.collect(Collectors.joining(","));
		return String.format(STATEMENT_NODE_FULLTEXT_IDX, name, label, key);
	}

	@Override
	public String statementForRelationshipFullTextIndex(String name, Iterable<RelationshipType> types, Iterable<String> keys) {
		String type = StreamSupport.stream(types.spliterator(), false)
				.map(RelationshipType::name)
				.map(Util::quote)
				.map(s -> String.format(PROPERTY_QUOTING_FORMAT, s))
				.collect(Collectors.joining(","));
		String key = StreamSupport.stream(keys.spliterator(), false)
				.map(Util::quote)
				.map(s -> String.format(PROPERTY_QUOTING_FORMAT, s))
				.collect(Collectors.joining(","));
		return String.format(STATEMENT_REL_FULLTEXT_IDX, name, type, key);
	}

	@Override
	public String statementForConstraint(String label, Iterable<String> keys, boolean ifNotExists) {
		String keysString = getPropertiesQuoted(keys);

		return String.format(STATEMENT_CONSTRAINTS, getIfNotExists(ifNotExists), Util.quote(label), keysString, Iterables.count(keys) > 1 ? "IS NODE KEY" : "IS UNIQUE");
	}

	private String getIfNotExists(boolean ifNotExists) {
		return ifNotExists ? " IF NOT EXISTS" : "";
	}


	private String getPropertiesQuoted(Iterable<String> keys) {
		return StreamSupport.stream(keys.spliterator(), false)
				.map(key -> "node." + CypherFormatterUtils.quote(key))
				.collect(Collectors.joining(", "));
	}

	private Set<String> getLabels(Node node) {
		Set<String> labels = StreamSupport.stream(node.getLabels().spliterator(), false)
				.map(Label::name)
				.collect(Collectors.toSet());
		if (labels.isEmpty()) {
			labels.add(CypherFormatterUtils.UNIQUE_ID_LABEL);
		}
		return labels;
	}
	
	public void groupRelationships(Iterable<Relationship> relationship, Map<String, Set<String>> uniqueConstraints, GraphDatabaseService db, TemplateCypher templateCypher) {
		Function<Relationship, Map<String, Object>> keyMapper = (rel) -> {
			try (Transaction tx = db.beginTx()) {
				rel = tx.getRelationshipById(rel.getId());
				Node start = rel.getStartNode();
				Set<String> startLabels = getLabels(start);

				// define the end labels
				Node end = rel.getEndNode();
				Set<String> endLabels = getLabels(end);

				// define the type
				String type = rel.getType().name();

				// create the path
				Map<String, Object> key = Util.map("type", type,
						"start", new AbstractMap.SimpleImmutableEntry<>(startLabels, CypherFormatterUtils.getNodeIdProperties(start, uniqueConstraints).keySet()),
						"end", new AbstractMap.SimpleImmutableEntry<>(endLabels, CypherFormatterUtils.getNodeIdProperties(end, uniqueConstraints).keySet()));

				tx.commit();
				return key;
			}
		};
		Map<Map<String, Object>, List<Relationship>> groupedData = StreamSupport.stream(relationship.spliterator(), true)
				.collect(Collectors.groupingByConcurrent(keyMapper));

		templateCypher.setGroupedRelationships(groupedData);
	}

	public void groupNodes(Iterable<Node> nodes, Map<String, Set<String>> uniqueConstraints,
											 GraphDatabaseService db, TemplateCypher templateCypher) {
		Function<Node, Map.Entry<Set<String>, Set<String>>> keyMapper = (node) -> {
			try (Transaction tx = db.beginTx()) {
				node = tx.getNodeById(node.getId());
				Set<String> idProperties = CypherFormatterUtils.getNodeIdProperties(node, uniqueConstraints).keySet();
				Set<String> labels = getLabels(node);
				tx.commit();
				return new AbstractMap.SimpleImmutableEntry<>(labels, idProperties);
			}
		};
		Map<Map.Entry<Set<String>, Set<String>>, List<Node>> groupedData = StreamSupport.stream(nodes.spliterator(), true)
				.collect(Collectors.groupingByConcurrent(keyMapper));
		templateCypher.setGroupedNodes(groupedData);

	}

	protected String mergeStatementForNode(CypherFormat cypherFormat, Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
		StringBuilder result = new StringBuilder(1000);
		result.append("MERGE ");
		result.append(CypherFormatterUtils.formatNodeLookup("n", node, uniqueConstraints, indexNames));
		if (node.getPropertyKeys().iterator().hasNext()) {
			String notUniqueProperties = CypherFormatterUtils.formatNotUniqueProperties("n", node, uniqueConstraints, indexedProperties, false);
			String notUniqueLabels = CypherFormatterUtils.formatNotUniqueLabels("n", node, uniqueConstraints);
			if (!"".equals(notUniqueProperties) || !"".equals(notUniqueLabels)) {
				result.append(cypherFormat.equals(CypherFormat.ADD_STRUCTURE) ? " ON CREATE SET " : " SET ");
				result.append(notUniqueProperties);
				result.append(!"".equals(notUniqueProperties) && !"".equals(notUniqueLabels) ? ", " : "");
				result.append(notUniqueLabels);
			}
		}
		result.append(";\n");
		return result.toString();
	}

	public String mergeStatementForRelationship(CypherFormat cypherFormat, Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties) {
		StringBuilder result = new StringBuilder(1000);
		result.append("MATCH ");
		result.append(CypherFormatterUtils.formatNodeLookup("n1", relationship.getStartNode(), uniqueConstraints, indexedProperties));
		result.append(", ");
		result.append(CypherFormatterUtils.formatNodeLookup("n2", relationship.getEndNode(), uniqueConstraints, indexedProperties));
		result.append(" MERGE (n1)-[r:" + CypherFormatterUtils.quote(relationship.getType().name()) + "]->(n2)");
		if (relationship.getPropertyKeys().iterator().hasNext()) {
			result.append(cypherFormat.equals(CypherFormat.UPDATE_STRUCTURE) ? " ON CREATE SET " : " SET ");
			result.append(CypherFormatterUtils.formatRelationshipProperties("r", relationship, false));
		}
		result.append(";\n");
		return result.toString();
	}

	public void closeUnwindNodes(String nodeClause, String setClause, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, Map.Entry<Set<String>, Set<String>> key, Node last) throws IOException {
		writeUnwindEnd(exportConfig, out);
		out.append(StringUtils.LF);
		out.append(nodeClause);

		String label = getUniqueConstrainedLabel(last, uniqueConstraints);
		out.append("(n:");
		out.append(Util.quote(label));
		out.append("{");
		writeSetProperties(out, key.getValue());
		out.append("}) ");
		out.append(setClause);
		out.append("n += row.properties");
		String addLabels = key.getKey().stream()
				.filter(l -> !l.equals(label))
				.map(Util::quote)
				.collect(Collectors.joining(":"));
		if (!addLabels.isEmpty()) {
			out.append(" SET n:");
			out.append(addLabels);
		}
		out.append(";");
		out.append(StringUtils.LF);
	}
	
	private void writeSetProperties(Writer out, Set<String> value) throws IOException {
		writeSetProperties(out, value, null);
	}

	private void writeSetProperties(Writer out, Set<String> value, String prefix) throws IOException {
		if (prefix == null) prefix = "";
		int size = value.size();
		for (String s: value) {
			--size;
			out.append(Util.quote(s) + ": row." + prefix + formatNodeId(s));
			if (size > 0) {
				out.append(", ");
			}
		}
	}

	public void closeUnwindRelationships(String relationshipClause, String setClause, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, Writer out, String start, String end, Map<String, Object> path, Relationship last) throws IOException {
		writeUnwindEnd(exportConfig, out);
		// match start node
		writeRelationshipMatchAsciiNode(last.getStartNode(), out, start, uniqueConstraints);

		// match end node
		writeRelationshipMatchAsciiNode(last.getEndNode(), out, end, uniqueConstraints);

		out.append(StringUtils.LF);

		// create the relationship (depends on the strategy)
		out.append(relationshipClause);
		out.append("(start)-[r:" + Util.quote(path.get("type").toString()) + "]->(end) ");
		out.append(setClause);
		out.append("r += row.properties;");
		out.append(StringUtils.LF);
	}

	private String formatNodeId(String key) {
		if (UNIQUE_ID_PROP.equals(key)) {
			key = "_id";
		}
		return Util.quote(key);
	}

	private void writeUnwindEnd(ExportConfig exportConfig, Writer out) throws IOException {
		out.append("]");
		if (exportConfig.getFormat() == ExportFormat.CYPHER_SHELL
				&& exportConfig.getOptimizationType() == ExportConfig.OptimizationType.UNWIND_BATCH_PARAMS) {
			out.append(StringUtils.LF);
			out.append("UNWIND $rows");
		}
		out.append(" AS row");
	}

	private void writeRelationshipMatchAsciiNode(Node node, Writer out, String key, Map<String, Set<String>> uniqueConstraints) throws IOException {
		String uniqueConstrainedLabel = getUniqueConstrainedLabel(node, uniqueConstraints);
		Set<String> uniqueConstrainedProps = getUniqueConstrainedProperties(uniqueConstraints, uniqueConstrainedLabel);

		out.append(StringUtils.LF);
		out.append("MATCH ");
		out.append("(");
		out.append(key);
		out.append(":");
		out.append(Util.quote(uniqueConstrainedLabel));
		out.append("{");
		writeSetProperties(out, uniqueConstrainedProps, key + ".");
		out.append("})");
	}
}

