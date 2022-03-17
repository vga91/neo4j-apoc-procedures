package apoc.bolt;

import apoc.Description;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.UriResolver;
import apoc.util.Util;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class Bolt2 {

    @Context
    public GraphDatabaseService db;

    @Procedure()
    @Description("apoc.bolt.load(url-or-key, statement, params, config) - access to other databases via bolt for read/write")
    public Stream<RowResult> load(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        if (params == null) params = Collections.emptyMap();
        BoltConfig boltConfig = new BoltConfig(config);


        UriResolver uri = new UriResolver(url, "bolt");
        uri.initialize();
        try {
            long time = System.currentTimeMillis();
            final Driver driver = GraphDatabase.driver(uri.getConfiguredUri(), uri.getToken(), boltConfig.getDriverConfig());
            final Session session = driver.session();
            final Stream<RowResult> result;
            if (boltConfig.isAddStatistics()) {
                result = Stream.of(new RowResult(toMap(runStatement(statement, session, params, boltConfig).consume().counters())));
            } else {
                result = getRowResultStream(session, params, statement, boltConfig);
            }
            return result.onClose(() -> {
                session.close();
                driver.close();
            });
        } catch (Exception e) {
            throw new RuntimeException("It's not possible to create a connection due to: " + e.getMessage());
        }
    }

    @Procedure()
    @Description("apoc.bolt.execute(url-or-key, statement, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> execute(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        Map<String, Object> configuration = new HashMap<>(config);
        configuration.put("readOnly", false);
        return load(url, statement, params, configuration);
    }

    private Result runStatement(@Name("statement") String statement, Session session, Map<String, Object> finalParams, BoltConfig boltConfig) {
//        return boltConfig.isReadOnly()
//                ? session.readTransaction((Transaction tx) -> (Result) tx.execute(statement, finalParams))
//                : session.writeTransaction((Transaction tx) -> (Result) tx.execute(statement, finalParams));
//        return boltConfig.isReadOnly()
                return session.readTransaction(tx -> {
            return (Result) tx.run(statement, finalParams);
        });
//                : session.writeTransaction((Transaction tx) -> (Result) tx.execute(statement, finalParams));
    }

    private Stream<RowResult> getRowResultStream(Session session, Map<String, Object> params, String statement, BoltConfig boltConfig) {
        Map<Long, VirtualNode> nodesCache = new ConcurrentHashMap<>();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(runStatement(statement, session, params, boltConfig), 0), true)
//                .map(RowResult::new);
                .map(record -> new RowResult(record.asMap(value -> convert(session, value, boltConfig, nodesCache))));
    }

    private Object convert(Session session, Object entity, BoltConfig boltConfig, Map<Long, VirtualNode> nodeCache) {
        if (entity instanceof Value) return convert(session, ((Value) entity).asObject(), boltConfig, nodeCache);
        if (entity instanceof Node) return toNode(entity, boltConfig, nodeCache);
        if (entity instanceof Relationship) return toRelationship(session, entity, boltConfig, nodeCache);
        if (entity instanceof Path) return toPath(session, entity, boltConfig, nodeCache);
        if (entity instanceof Collection) return toCollection(session, (Collection) entity, boltConfig, nodeCache);
        if (entity instanceof Map) return toMap(session, (Map<String, Object>) entity, boltConfig, nodeCache);
        return entity;
    }

    private Object toMap(Session session, Map<String, Object> entity, BoltConfig boltConfig, Map<Long, VirtualNode> nodeCache) {
        return entity.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry(entry.getKey(), convert(session, entry.getValue(), boltConfig, nodeCache)))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private Object toCollection(Session session, Collection entity, BoltConfig boltConfig, Map<Long, VirtualNode> nodeCache) {
        return entity.stream()
                .map(elem -> convert(session, elem, boltConfig, nodeCache))
                .collect(Collectors.toList());
    }

    private Object toNode(Object value, BoltConfig boltConfig, Map<Long, VirtualNode> nodesCache) {
        Node node = (Node) value;
//        if (value instanceof Value) {
//            node = ((InternalEntity) value).asValue().asNode();
//        } else if (value instanceof Node) {
//            node = (Node) value;
//        } else {
//            throw getUnsupportedConversionException(value);
//        }
        if (boltConfig.isVirtual()) {
            final Label[] labels = getLabelsAsArray(node);
            return nodesCache.computeIfAbsent(node.getId(), (id) -> VirtualNode.from(node));
        } else {
            return Util.map("entityType", "NODE", "labels", node.getLabels(), "id", node.getId(), "properties", node.getAllProperties());
        }
    }

    private Object toRelationship(Session session, Object value, BoltConfig boltConfig, Map<Long, VirtualNode> nodesCache) {
        Relationship relationship = (Relationship) value;
//        if (value instanceof Value) {
//            relationship = ((InternalEntity) value).asValue().asRelationship();
//        } else if (value instanceof Relationship) {
//            relationship = (Relationship) value;
//        } else {
//            throw getUnsupportedConversionException(value);
//        }
        if (boltConfig.isVirtual()) {
            final VirtualNode start;
            final VirtualNode end;
//            if (boltConfig.isWithRelationshipNodeProperties()) {
//                final Function<Long, VirtualNode> retrieveNode = (id) -> {
//                    final Node node = (Node) session.readTransaction(tx -> tx.run("MATCH (n) WHERE id(n) = $id RETURN n",
//                            Collections.singletonMap("id", id)))
//                            .single()
//                            .get("n")
//                            .asNode();
//                    return VirtualNode.from(node);
//                };
//                start = nodesCache.computeIfAbsent(relationship.getStartNodeId(), retrieveNode);
//                end = nodesCache.computeIfAbsent(relationship.getEndNodeId(), retrieveNode);
//            } else {
                start = nodesCache.computeIfAbsent(relationship.getStartNodeId(), (id) -> VirtualNode.from(relationship.getStartNode()));
                end = nodesCache.computeIfAbsent(relationship.getEndNodeId(), (id) -> VirtualNode.from(relationship.getEndNode()));
//            }
            // todo - Util fun VirtualRelationship.from(relationship)
            return VirtualRelationship.from(start, end, relationship);
        } else {
            return Util.map("entityType", "RELATIONSHIP", "type", relationship.getType(), "id", relationship.getId(), "start", relationship.getStartNode(), "end", relationship.getEndNode(), "properties", relationship.getAllProperties());
        }
    }

    private ClassCastException getUnsupportedConversionException(Object value) {
        return new ClassCastException("Conversion from class " + value.getClass().getName() + " not supported");
    }

    private Label[] getLabelsAsArray(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(), false).toArray(Label[]::new);
    }

    private Object toPath(Session session, Object value, BoltConfig boltConfig, Map<Long, VirtualNode> nodesCache) {
        List<Object> entityList = new LinkedList<>();
        Path path;
//        if (value instanceof Value) {
//            path = ((InternalEntity) value).asValue().asPath();
//        } else if (value instanceof Path) {
            path = (Path) value;
//        } else {
//            throw getUnsupportedConversionException(value);
//        }


        // todo - forse non serve...
//        path.forEach(p -> {
//            entityList.add(toNode(p, boltConfig, nodesCache));
//            entityList.add(toRelationship(session, p.relationship(), boltConfig, nodesCache));
//            entityList.add(toNode(p.end(), boltConfig, nodesCache));
//        });
        return entityList;
    }

    private Map<String, Object> toMap(SummaryCounters resultSummary) {
        return map(
                "nodesCreated", resultSummary.nodesCreated(),
                "nodesDeleted", resultSummary.nodesDeleted(),
                "labelsAdded", resultSummary.labelsAdded(),
                "labelsRemoved", resultSummary.labelsRemoved(),
                "relationshipsCreated", resultSummary.relationshipsCreated(),
                "relationshipsDeleted", resultSummary.relationshipsDeleted(),
                "propertiesSet", resultSummary.propertiesSet(),
                "constraintsAdded", resultSummary.constraintsAdded(),
                "constraintsRemoved", resultSummary.constraintsRemoved(),
                "indexesAdded", resultSummary.indexesAdded(),
                "indexesRemoved", resultSummary.indexesRemoved()
        );
    }
}