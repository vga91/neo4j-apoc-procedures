package apoc.periodic;

import apoc.trigger.TriggerMetadata;
import org.apache.commons.collections.MapUtils;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PeriodicCommitHandler extends LifecycleAdapter implements TransactionEventListener<Void> {
    private final GraphDatabaseService db;
    private final DatabaseManagementService service;
    private final Map<String, Map<String, Object>> txDataMap = new ConcurrentHashMap<>();

    private String handleTransaction = null;

    @Override
    public void start() {
        this.service.registerTransactionEventListener(db.databaseName(), this);
    }

    @Override
    public void stop() {
        txDataMap.clear();
        this.service.unregisterTransactionEventListener(db.databaseName(), this);
    }

    @Override
    public Void beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
        // when is a transaction from a periodic.commit with {extractData: true}
        if(this.handleTransaction != null) {
            Map<String, Object> currentData = TriggerMetadata.from(data, false, true).toMapPeriodic();
            // aggregate periodic.commit results
            this.txDataMap.compute(this.handleTransaction, (k, v) -> mergeTransactionMaps(v, currentData));
            this.handleTransaction = null;
        }
        return null;
    }

    @Override
    public void afterCommit(TransactionData data, Void state, GraphDatabaseService databaseService) {}

    @Override
    public void afterRollback(TransactionData data, Void state, GraphDatabaseService databaseService) {}

    
    public PeriodicCommitHandler(GraphDatabaseService db, DatabaseManagementService service) {
        this.db = db;
        this.service = service;
    }
    
    public synchronized long executeNumericResultStatement(String statement, Map<String, Object> parameters, String uuid) {
        try (Transaction transaction = this.db.beginTx()) {
            final Result result = transaction.execute(statement, parameters);
            String column = Iterables.single(result.columns());
            final long numericResult = result.columnAs(column).stream().mapToLong(o -> (long) o).sum();
            this.handleTransaction = uuid;
            transaction.commit();
            this.handleTransaction = null;
            return numericResult;
        }
    }

    public synchronized Map<String, Object> getTxData(String uuid) {
        // with extractData: false
        if (uuid == null) {
            return null;
        }
        return txDataMap.remove(uuid);
    }

    private synchronized static Map<String, Object> mergeTransactionMaps(Map<String, Object> mapStart, Map<String, Object> mapEnd) {
        if (MapUtils.isEmpty(mapStart)) {
            return mapEnd;
        }
        if (MapUtils.isEmpty(mapEnd)) {
            return mapStart;
        }
        return Stream.of(mapStart, mapEnd)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> {
                    if (v1 instanceof List) {
                        ((List) v1).addAll((List) v2);
                        return v1;
                    } else {
                        return mergeTransactionMaps(((Map<String, Object>) v1), ((Map<String, Object>) v2));
                    }
                }));
    }
}
