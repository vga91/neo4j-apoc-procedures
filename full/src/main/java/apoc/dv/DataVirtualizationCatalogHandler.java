package apoc.dv;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import apoc.util.SystemDbUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.SystemDbUtil.currentDb;
import static apoc.util.SystemDbUtil.otherDb;

public class DataVirtualizationCatalogHandler extends LifecycleAdapter implements DatabaseEventListener {
    private static final String NAME = "dv";

    private final GraphDatabaseService db;
    private final Log log;

    public DataVirtualizationCatalogHandler(GraphDatabaseService db, Log log) {
        this.db = db;
        this.log = log;
    }

    private <T> T withOtherDb(Function<Transaction, T> action) {
        return otherDb(db, NAME, action);
    }

    private <T> T withThisDb(Function<Transaction, T> action) {
        return currentDb(db, NAME, action);
    }

    public VirtualizedResource add(VirtualizedResource vr) {
        return withThisDb(tx -> {
            // todo - common
            Node node = Util.mergeNode(tx, SystemLabels.DataVirtualizationCatalog, null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), vr.name));
            node.setProperty(SystemPropertyKeys.data.name(), JsonUtil.writeValueAsString(vr));
            return vr;
        });
    }

    public VirtualizedResource get(String name) {
        return withThisDb(tx -> {
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
        withThisDb(tx -> {
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
        return withThisDb(tx ->
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

        SystemDbUtil.migrateInfo(db, SystemLabels.DataVirtualizationCatalog);
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
