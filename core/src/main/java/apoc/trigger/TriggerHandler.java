package apoc.trigger;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.SystemDbUtil.KEY_CURRENT_DB;
import static apoc.util.SystemDbUtil.todoThisDb;
import static apoc.util.SystemDbUtil.todoOtherDb;

public class TriggerHandler extends LifecycleAdapter implements DatabaseEventListener, TransactionEventListener<Void> {
    private static final String NAME = "trigger";

//    @Override
//    public void available() {
//        System.out.println("trigger key available= " + apocConfig.getBoolean(KEY_CURRENT_DB, false));
//    }
//
//    @Override
//    public void unavailable() {
//
//    }
//
//    @Override
//    public void init() {
//        System.out.println("trigger key init= " + apocConfig.getBoolean(KEY_CURRENT_DB, false));
//        System.out.println("TriggerHandler.init");
//        try(final Transaction transaction = db.beginTx()) {
//            final Node bbb = transaction.findNode(Label.label("bbb"), "a", 1);
//            System.out.println("bbb = " + bbb);
//            transaction.commit();
//        }
//    }

    @Override
    public void databaseStart(DatabaseEventContext eventContext) {
        System.out.println("TriggerHandler.databaseStart " + eventContext.getDatabaseName());
        
        System.out.println("trigger key aaa= " + apocConfig.getBoolean("apoc.trigger.persist", true));
        System.out.println("trigger key databaseStart= " + apocConfig.getBoolean(KEY_CURRENT_DB, false));

        // final boolean isSystemDatabase = db.databaseName().equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);

        try(final Transaction transaction = db.beginTx()) {
            final Node ccc = transaction.findNode(Label.label("ccc"), "a", 1);
            System.out.println("ccc = " + ccc);
            transaction.commit();
        }

        // todo - forse conviene aprire una sola transazione...
        final ResourceIterator<Node> nodes = withOtherDb(tx -> tx.findNodes(
                SystemLabels.ApocTrigger, SystemPropertyKeys.database.name(), db.databaseName()));
        
        final ResourceIterator<Node> metaNodes = withOtherDb(tx -> tx.findNodes(
                SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName()));

        withDb(tx -> {
            nodes.forEachRemaining(node -> {
                Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                        Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                        Pair.of(SystemPropertyKeys.name.name(), node.getProperty(SystemPropertyKeys.name.name()))
                );
            });

            metaNodes.forEachRemaining(node -> {
                Util.mergeNode(tx, SystemLabels.ApocTriggerMeta, null,
                        Pair.of(SystemPropertyKeys.database.name(), db.databaseName()));
            });
            return null;
        });
    }

    @Override
    public void databaseShutdown(DatabaseEventContext eventContext) {

    }

    @Override
    public void databasePanic(DatabaseEventContext eventContext) {
        // todo - maybe do nothing
    }

    private enum Phase {before, after, rollback, afterAsync}

    public static final String TRIGGER_REFRESH = "apoc.trigger.refresh";

    private final ConcurrentHashMap<String, Map<String,Object>> activeTriggers = new ConcurrentHashMap();
    private final Log log;
    private final GraphDatabaseService db;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;
    private final Pools pools;
    private final JobScheduler jobScheduler;

    private long lastUpdate;

    private JobHandle restoreTriggerHandler;

    private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";
    private final ThrowingFunction<Context, Transaction, ProcedureException> transactionComponentFunction;

    public TriggerHandler(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                          ApocConfig apocConfig, Log log, GlobalProcedures globalProceduresRegistry,
                          Pools pools, JobScheduler jobScheduler) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.apocConfig = apocConfig;
        this.log = log;
        transactionComponentFunction = globalProceduresRegistry.lookupComponentProvider(Transaction.class, true);
        this.pools = pools;
        this.jobScheduler = jobScheduler;
    }

    private boolean isEnabled() {
        return apocConfig.getBoolean(APOC_TRIGGER_ENABLED);
    }

    public void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    // todo - all'inizio dev'essere un moveOn currentDb
    private void updateCache() {
        activeTriggers.clear();

        lastUpdate = System.currentTimeMillis();

        // todo - forse si dovrebbe aspettare che sia disponibile il db...
        // todo - ma forse... nel start non va bene... bensì nel available

        withDb(tx -> {
            // todo - db.databaseName() è un info che non serve in current db
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName()).forEachRemaining(
                    node -> activeTriggers.put(
                            (String) node.getProperty(SystemPropertyKeys.name.name()),
                            MapUtil.map(
                                    "statement", node.getProperty(SystemPropertyKeys.statement.name()),
                                    "selector", Util.fromJson((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class),
                                    "params", Util.fromJson((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class),
                                    "paused", node.getProperty(SystemPropertyKeys.paused.name())
                            )
                    )
            );
            return null;
        });

        reconcileKernelRegistration();
    }

    /**
     * There is substantial memory overhead to the kernel event system, so if a user has enabled apoc triggers in
     * config, but there are no triggers set up, unregister to let the kernel bypass the event handling system.
     *
     * For most deployments this isn't an issue, since you can turn the config flag off, but in large fleet deployments
     * it's nice to have uniform config, and then the memory savings on databases that don't use triggers is good.
     */
    private synchronized void reconcileKernelRegistration() {
        // Register if there are triggers
        if (activeTriggers.size() > 0) {
            // This gets called every time triggers update; only register if we aren't already
            if(registeredWithKernel.compareAndSet(false, true)) {
                databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
            }
        } else {
            // This gets called every time triggers update; only unregister if we aren't already
            if(registeredWithKernel.compareAndSet(true, false)) {
                databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
            }
        }
    }

    public Map<String, Object> add(String name, String statement, Map<String,Object> selector) {
        return add(name, statement, selector, Collections.emptyMap());
    }

    // todo - potrei usare questo merge node,,,
    public Map<String, Object> add(String name, String statement, Map<String,Object> selector, Map<String,Object> params) {
        checkEnabled();
        Map<String, Object> previous = activeTriggers.get(name);

        withDb(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), name));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);
            setLastUpdate(tx);
            return null;
        });

        updateCache();
        return previous;
    }

    public Map<String, Object> remove(String name) {
        checkEnabled();
        Map<String, Object> previous = activeTriggers.remove(name);

        withDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                    .forEachRemaining(node -> node.delete());
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return previous;
    }

    public Map<String, Object> updatePaused(String name, boolean paused) {
        checkEnabled();
        withDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                    .forEachRemaining(node -> node.setProperty(SystemPropertyKeys.paused.name(), paused));
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return activeTriggers.get(name);
    }

    public Map<String, Object> removeAll() {
        checkEnabled();
        Map<String, Object> previous = activeTriggers
                .entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        withDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName() )
                    .forEachRemaining(node -> node.delete());
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return previous;
    }

    public Map<String,Map<String,Object>> list() {
        checkEnabled();
        return Map.copyOf(activeTriggers);
    }

    @Override
    public Void beforeCommit(TransactionData txData, Transaction transaction, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.before)) {
            executeTriggers(transaction, txData, Phase.before);
        }
        return null;
    }

    @Override
    public void afterCommit(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.after)) {
            try (Transaction tx = db.beginTx()) {
                executeTriggers(tx, txData, Phase.after);
                tx.commit();
            }
        }
        afterAsync(txData);
    }

    private void afterAsync(TransactionData txData) {
        if (hasPhase(Phase.afterAsync)) {
            TriggerMetadata triggerMetadata = TriggerMetadata.from(txData, true);
            Util.inTxFuture(pools.getDefaultExecutorService(), db, (inner) -> {
                executeTriggers(inner, triggerMetadata.rebind(inner), Phase.afterAsync);
                return null;
            });
        }
    }

    @Override
    public void afterRollback(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.rollback)) {
            try (Transaction tx = db.beginTx()) {
                executeTriggers(tx, txData, Phase.rollback);
                tx.commit();
            }
        }
    }

    private boolean hasPhase(Phase phase) {
        return activeTriggers.values().stream()
                .map(data -> (Map<String, Object>) data.get("selector"))
                .anyMatch(selector -> when(selector, phase));
    }

    private void executeTriggers(Transaction tx, TransactionData txData, Phase phase) {
        executeTriggers(tx, TriggerMetadata.from(txData, false), phase);
    }

    private void executeTriggers(Transaction tx, TriggerMetadata triggerMetadata, Phase phase) {
        Map<String,String> exceptions = new LinkedHashMap<>();
        activeTriggers.forEach((name, data) -> {
            Map<String, Object> params = triggerMetadata.toMap();
            if (data.get("params") != null) {
                params.putAll((Map<String, Object>) data.get("params"));
            }
            Map<String, Object> selector = (Map<String, Object>) data.get("selector");
            if ((!(boolean)data.get("paused")) && when(selector, phase)) {
                try {
                    params.put("trigger", name);
                    Result result = tx.execute((String) data.get("statement"), params);
                    Iterators.count(result);
                } catch (Exception e) {
                    log.warn("Error executing trigger " + name + " in phase " + phase, e);
                    exceptions.put(name, e.getMessage());
                }
            }
        });
        if (!exceptions.isEmpty()) {
            throw new RuntimeException("Error executing triggers "+exceptions.toString());
        }
    }

    private boolean when(Map<String, Object> selector, Phase phase) {
        if (selector == null) return phase == Phase.before;
        return Phase.valueOf(selector.getOrDefault("phase", "before").toString()) == phase;
    }


//    public void ifPersistDo(Runnable runnable) {
//        if (!apocConfig.getBoolean(TRIGGER_PERSIST, true)) {
//            removeAll();
//            return;
//        }
//        runnable.run();
//    }

    @Override
    public void start() throws Exception {
        System.out.println("trigger key start aaa= " + apocConfig.getBoolean("apoc.trigger.persist", true));


        final boolean aBoolean = apocConfig.getBoolean(KEY_CURRENT_DB, false);
        System.out.println("trigger key = " + aBoolean);

//        databaseManagementService.registerDatabaseEventListener(this);

        // non è che ci passa più volte???

//        try(final Transaction transaction = db.beginTx()) {
//            final Node aaa = transaction.findNode(Label.label("aaa"), "a", 1);
//            System.out.println("aaa = " + aaa);
//            transaction.commit();
//        }



//        move() <-- todo: capire se fare generico per tuttele funzionalità o no...
        updateCache();
        long refreshInterval = apocConfig().getInt(TRIGGER_REFRESH, 60000);
        restoreTriggerHandler = jobScheduler.scheduleRecurring(Group.STORAGE_MAINTENANCE, () -> {
            if (getLastUpdate() > lastUpdate) {
                updateCache();
            }
        }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        System.out.println("trigger key stop aaa= " + apocConfig.getBoolean("apoc.trigger.persist", true));

        if(registeredWithKernel.compareAndSet(true, false)) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
        }
        if (restoreTriggerHandler != null) {
            restoreTriggerHandler.cancel();
        }
//        databaseManagementService.unregisterDatabaseEventListener(this);
    }

    private <T> T withDb(Function<Transaction, T> action) {
        return todoThisDb(db, NAME, action);
//        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
    }

    private <T> T withOtherDb(Function<Transaction, T> action) {
        return todoOtherDb(db, NAME, action);
//        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
    }

    private long getLastUpdate() {
        return withDb(tx -> {
            Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
            return node == null ? 0L : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
        });
    }

    private void setLastUpdate(Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocTriggerMeta);
            node.setProperty(SystemPropertyKeys.database.name(), db.databaseName());
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }

}
