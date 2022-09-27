package apoc.trigger;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class TriggerUtils {
    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public static Map<String, Object> toTriggerInfo(Node node) {
        HashSet<String> nodeKeys = new HashSet();
        node.getPropertyKeys().iterator().forEachRemaining(key -> nodeKeys.add( key ));
        HashMap<String, Object> result = new HashMap();

        if (nodeKeys.contains("statement"))
        {
            result.put( "statement", node.getProperty( SystemPropertyKeys.statement.name() ) );
        }
        if (nodeKeys.contains("selector"))
        {
            result.put( "selector", Util.fromJson((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class));
        }
        if (nodeKeys.contains("params"))
        {
            result.put( "params", Util.fromJson((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class));
        }
        if (nodeKeys.contains("paused"))
        {
            result.put( "paused", node.getProperty(SystemPropertyKeys.paused.name()));
        }

        return result;
    }

    private static boolean isEnabled() {
        return apocConfig().getBoolean(APOC_TRIGGER_ENABLED);
    }

    public static void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public static Map<String, Object> add(String databaseName, String triggerName, String statement, Map<String,Object> selector, Map<String,Object> params) {
        checkEnabled();
        HashMap<String, Object> previous = new HashMap();

        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                    Pair.of(SystemPropertyKeys.database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.name.name(), triggerName));
            previous.putAll(TriggerUtils.toTriggerInfo(node));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);
            setLastUpdate(databaseName, tx);
            return null;
        });

        return previous;
    }

    public static Map<String, Object> remove(String databaseName, String triggerName) {
        checkEnabled();
        HashMap<String, Object> previous = new HashMap();

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                            SystemPropertyKeys.database.name(), databaseName,
                            SystemPropertyKeys.name.name(), triggerName)
                    .forEachRemaining(node ->
                            {
                                previous.putAll(TriggerUtils.toTriggerInfo(node));
                                node.delete();
                            }
                    );
            setLastUpdate(databaseName, tx);

            return null;
        });

        return previous;
    }

    public static Map<String, Object> updatePaused(String databaseName, String name, boolean paused) {
        checkEnabled();
        HashMap<String, Object> result = new HashMap();

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                            SystemPropertyKeys.database.name(), databaseName,
                            SystemPropertyKeys.name.name(), name)
                    .forEachRemaining(node ->
                    {
                        node.setProperty( SystemPropertyKeys.paused.name(), paused );
                        result.putAll(TriggerUtils.toTriggerInfo(node));
                    });
            
            // in updatePaused we don't need setLastUpdate because reconcileKernelRegistration() only check trigger existence, 
            //  not if they are paused or started.

            return null;
        });

        return result;
    }

    public static Map<String, Object> removeAll(String databaseName) {
        checkEnabled();
        HashMap<String, Object> previous = new HashMap();

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx)
                    .forEachRemaining(node -> {
                        String triggerName = (String) node.getProperty(SystemPropertyKeys.name.name());
                        previous.put(triggerName, TriggerUtils.toTriggerInfo(node));
                        node.delete();
                    });
            setLastUpdate(databaseName, tx);

            return null;
        });

        return previous;
    }

    public static ResourceIterator<Node> getTriggerNodes(String databaseName, Transaction tx) {
        return tx.findNodes(
                SystemLabels.ApocTrigger, SystemPropertyKeys.database.name(), 
                databaseName);
    }

    public static <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    private static void setLastUpdate(String databaseName, Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName);
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocTriggerMeta);
            node.setProperty(SystemPropertyKeys.database.name(), databaseName);
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }
    // todo - made
//    private void updateCache() {
//        activeTriggers.clear();
//        System.out.println("updates cache");
//        lastUpdate = System.currentTimeMillis();
//
//        withSystemDb(tx -> {
//            tx.findNodes(SystemLabels.ApocTrigger,
//                    SystemPropertyKeys.database.name(), db.databaseName()).forEachRemaining(
//                    node -> {
//                        System.out.println("there are nodes to update the cache");
//                        activeTriggers.put(
//                                (String) node.getProperty(SystemPropertyKeys.name.name()),
//                                TriggerUtils.toTriggerInfo(node)
//                        );
//                    }
//            );
//            return null;
//        });
//
//        reconcileKernelRegistration();
//    }
}
