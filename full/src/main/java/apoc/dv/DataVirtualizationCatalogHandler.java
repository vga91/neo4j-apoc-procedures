package apoc.dv;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.SystemDbUtil.todoThisDb;

// todo - questi non vengono fatti al riavvio
public class DataVirtualizationCatalogHandler implements DatabaseEventListener { // todo - forse dovrei registrare anche questo??
    private static final String NAME = "dv";

    private final GraphDatabaseService db;
//    private final GraphDatabaseService systemDb;
    private final Log log;

    public DataVirtualizationCatalogHandler(GraphDatabaseService db, /*GraphDatabaseService systemDb, */Log log) {
        this.db = db;
//        this.systemDb = systemDb;
        this.log = log;
    }


    private <T> T todo4(Function<Transaction, T> action) {
        return todoThisDb(db, NAME, action);
//        try (Transaction tx = systemDb.beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
    }

    public VirtualizedResource add(VirtualizedResource vr) {
        return todo4(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.DataVirtualizationCatalog, null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), vr.name));
            node.setProperty(SystemPropertyKeys.data.name(), JsonUtil.writeValueAsString(vr));
            return vr;
        });
    }

    public VirtualizedResource get(String name) {
        return todo4(tx -> {
            final List<Node> nodes = tx.findNodes(SystemLabels.DataVirtualizationCatalog,
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                .stream()
                .collect(Collectors.toList());
            if (nodes.size() > 1) {
                throw new RuntimeException("More than 1 result");
            }
            try {
                Node node = nodes.get(0);
                Map<String, Object> map = JsonUtil.OBJECT_MAPPER.readValue(node.getProperty(SystemPropertyKeys.data.name()).toString(), Map.class);
                return VirtualizedResource.from(name, map);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Stream<VirtualizedResource> remove(String name) {
        todo4(tx -> {
            tx.findNodes(SystemLabels.DataVirtualizationCatalog,
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                .stream()
                .forEach(Node::delete);
            return null;
        });
        return list();
    }

    public Stream<VirtualizedResource> list() {
        // todo - questo... è comune a tutti in realta
        return todo4(tx ->
                getNodes(tx)
                .stream()
                .map(node -> {
                    try {
                        Map<String, Object> map = JsonUtil.OBJECT_MAPPER.readValue(node.getProperty(SystemPropertyKeys.data.name()).toString(), Map.class);
                        String name = node.getProperty(SystemPropertyKeys.name.name()).toString();
                        return VirtualizedResource.from(name, map);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList())
                .stream());
    }

    private ResourceIterator<Node> getNodes(Transaction tx) {
        return tx.findNodes(SystemLabels.DataVirtualizationCatalog,
                SystemPropertyKeys.database.name(), db.databaseName());
    }

    @Override
    public void databaseStart(DatabaseEventContext eventContext) {
        System.out.println("DataVirtualizationCatalogHandler.databaseStart" + eventContext.getDatabaseName());
    }

    @Override
    public void databaseShutdown(DatabaseEventContext eventContext) {
        System.out.println("DataVirtualizationCatalogHandler.databaseShutdown" + eventContext.getDatabaseName());
    }

    @Override
    public void databasePanic(DatabaseEventContext eventContext) {
        System.out.println("DataVirtualizationCatalogHandler.databasePanic" + eventContext.getDatabaseName());
    }
}
