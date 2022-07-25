package apoc.util;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDbUtil {
    public static class NodeInfo {
        public Label primaryLabel;
        public Label additionalLabel;
        public Pair<String, Object>[] pairs;
        public Map<String, Object> onCreateMap;

        public NodeInfo(Label primaryLabel, Label additionalLabel,
                        Pair<String, Object>[] pairs,
                        Map<String, Object> onCreateMap) {
            this.primaryLabel = primaryLabel;
            this.additionalLabel = additionalLabel;
            this.pairs = pairs;
            this.onCreateMap = onCreateMap;
        }
    }
    
    public static final String KEY_THIS_DB = "apoc.storethisdb";

    // todo - testo questa qua.. todo2 - ma in fondo non mi serve GraphDatabaseService, posso fare in SystemDbUtilsTest
    public static boolean isCurrentDb(String dbName, String featureName) {
        // todo - forse non serve.. verificare
        if (dbName.equals(SYSTEM_DATABASE_NAME)) {
            return false;
        }
        final String currentDbNameKey = String.format("%s.%s", KEY_THIS_DB, dbName);
        final String currentDbAndFeatureKey = String.format("%s.%s.%s", KEY_THIS_DB, dbName, featureName);

        return apocConfig().getBoolean(currentDbAndFeatureKey,
                apocConfig().getBoolean(currentDbNameKey,
                        apocConfig().getBoolean(KEY_THIS_DB, false))
        );
    }

    public static void migrateInfo(GraphDatabaseService db, SystemLabels label) {
        // todo - node -> List.of(Pair.of(SystemPropertyKeys.name.name(), node.getProperty(SystemPropertyKeys.name.name()))) alla fine, può essere il default...
        migrateInfo(db, label, node -> List.of(Pair.of(SystemPropertyKeys.name.name(), node.getProperty(SystemPropertyKeys.name.name()))));
    }

    public static void migrateInfo(GraphDatabaseService db, SystemLabels label, Function<Node, List<Pair>> mergePairs) {
        migrateInfo(db, label, mergePairs, node -> null, tx -> Collections.emptyList());
    }

    public static void migrateInfo(GraphDatabaseService db, SystemLabels label, Function<Node, List<Pair>> mergePairs, Function<Node, Label> additionalLabel, Function<Transaction, List<NodeInfo>> action) {
        if (!Util.isWriteableInstance(db)) {
            return;
        }
        
        final String featureName = label.getFeatureName();

        final List<NodeInfo> nodes = todoOtherDb(db, featureName, tx -> {
            try {
                final List<NodeInfo> collectCommon = getListNodeInfos(tx, db, label, additionalLabel, mergePairs);
    
                List<NodeInfo> collect = action.apply(tx);
                collectCommon.addAll(collect);
                System.out.println("collect = " + collect);
                return collectCommon;
            } catch (Exception e) {
                System.out.println("migrateInfos e = " + e);
                throw new RuntimeException(e);
            }
        });
        
        todoThisDb(db, featureName, tx -> {
            nodes.forEach(node -> {
                Util.mergeNode(tx, node.primaryLabel, node.additionalLabel, node.onCreateMap, Map.of(), node.pairs);
            });
            return null;
        });
        System.out.println("SystemDbUtil.migrateInfos");
    }

    public static List<NodeInfo> getListNodeInfos(Transaction tx, GraphDatabaseService db, SystemLabels label, Function<Node, Label> additionalLabelFun, Function<Node, List<Pair>> mergePairs) {
        System.out.println("SystemDbUtil.getListNodeInfos -- init");
        return tx.findNodes(label,SystemPropertyKeys.database.name(), db.databaseName())
                .stream()
                .map(node -> {
                    try {
                        final List<Pair> pairs = new ArrayList<>(mergePairs.apply(node));
                        pairs.add(Pair.of(SystemPropertyKeys.database.name(), db.databaseName()));

                        final Map<String, Object> allProperties = node.getAllProperties();
                        final Label additionalLabel = additionalLabelFun.apply(node);
                        
                        // delete source node
                        node.delete();

                        // -- retrieve infos
                        return new NodeInfo(label, additionalLabel, pairs.toArray(Pair[]::new), allProperties);
                    } catch (Exception e) {
                        System.out.println("getListNodeInfos e = " + e);
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    public static <T> T todoOtherDb(GraphDatabaseService db, String featureName, Function<Transaction, T> action) {
        final GraphDatabaseService currentDb = isCurrentDb(db.databaseName(), featureName)
                ? apocConfig().getSystemDb() : db;

        return getTransaction(action, currentDb);
    }

    private static <T> T getTransaction(Function<Transaction, T> action, GraphDatabaseService currentDb) {
        try (Transaction tx = currentDb.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        } catch (Exception e) {
            System.out.println("getTransaction e = " + e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T todoThisDb(GraphDatabaseService db, String featureName, Function<Transaction, T> action) {
        final GraphDatabaseService currentDb = isCurrentDb(db.databaseName(), featureName)
                ? db : apocConfig().getSystemDb();

        return getTransaction(action, currentDb);
    }
}
