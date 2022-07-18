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
    
    public static class ProvaMerge { // TODO - METTERLO IN SYSTEMDBUTILS???
        public Label primaryLabel;
        public Label additionalLabel;
        public Pair<String, Object>[] pairs;
        public Map<String, Object> onCreateMap;

        public ProvaMerge(Label primaryLabel, Label additionalLabel,
                          Pair<String, Object>[] pairs,
                          Map<String, Object> onCreateMap) {
            this.primaryLabel = primaryLabel;
            this.additionalLabel = additionalLabel;
            this.pairs = pairs;
            this.onCreateMap = onCreateMap;
        }
    }
    
    public static final String KEY_THIS_DB = "apoc.storethisdb"; // todo - e se lo chiamassi storethisdb????
    
//    public static final 
    
    // todo - SystemLabel.ApocCypherProcedures --> appartiene a tutti, posso prendere questo come feature
    // todo - creare un enum o qualcosa di simiie con le key
//    private static final Map<String, SystemLabels> featureMap = Map.of("custom", )

    public static boolean isCurrentDb(GraphDatabaseService db, String featureName) {
        // todo - forse non serve.. verificare
        if (db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
            return false;
        }
        final String currentDbNameKey = String.format("%s.%s", KEY_THIS_DB, db.databaseName());
        final String currentDbAndFeatureKey = String.format("%s.%s.%s", KEY_THIS_DB, db.databaseName(), featureName);

        return apocConfig().getBoolean(currentDbAndFeatureKey,
                apocConfig().getBoolean(currentDbNameKey,
                        apocConfig().getBoolean(KEY_THIS_DB, false))
        );
    }

    public static void migrateInfos(GraphDatabaseService db, SystemLabels label) {
        migrateInfos(db, label, node -> null, tx -> Collections.emptyList());
    }

    public static void migrateInfos(GraphDatabaseService db, SystemLabels label, Function<Node, Label> additionalLabel, Function<Transaction, List<ProvaMerge>> action) {
        final String featureName = label.getFeatureName();

        final List<ProvaMerge> nodes = todoOtherDb(db, featureName, tx -> {
            List<ProvaMerge> collect = action.apply(tx);

            final List<ProvaMerge> collect1 = getProvaMergeStream(tx, db, label, additionalLabel, 
                    node -> List.of(Pair.of(SystemPropertyKeys.name.name(), node.getProperty(SystemPropertyKeys.name.name()))));
//                    .collect(Collectors.toList());

            // todo - decommentare
            collect.addAll(collect1);
            return collect;
        });
        
        todoThisDb(db, featureName, tx -> {
            nodes.forEach(node -> {
                Util.mergeNode(tx, node.primaryLabel, node.additionalLabel, node.onCreateMap, Map.of(), node.pairs);
            });
            return null;
        });
    }

    public static List<ProvaMerge> getProvaMergeStream(Transaction tx, GraphDatabaseService db, SystemLabels label, Function<Node, Label> additionalLabel, Function<Node, List<Pair>> mergePairs) {
        return tx.findNodes(label,SystemPropertyKeys.database.name(), db.databaseName())
                .stream()
                .map(node -> {
                    final List<Pair> pairs = new ArrayList<>(mergePairs.apply(node));
                    pairs.add(Pair.of(SystemPropertyKeys.database.name(), db.databaseName()));
//                    final Pair[] pairs = {Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
//                            Pair.of(SystemPropertyKeys.name.name(), node.getProperty(SystemPropertyKeys.name.name()))};
                    final Map<String, Object> allProperties = node.getAllProperties();

                    node.delete();

                    // -- retrieve infos
                    return new ProvaMerge(label, additionalLabel.apply(node), pairs.toArray(Pair[]::new), allProperties);
                })
                .collect(Collectors.toList());
    }

    //CALL apoc.refactor.mergeNodes([f,b])

    public static <T> T todoOtherDb(GraphDatabaseService db, String featureName, Function<Transaction, T> action) {
        final GraphDatabaseService currentDb = isCurrentDb(db, featureName)
                ? apocConfig().getSystemDb() : db;

        return getT(action, currentDb);
    }

    private static <T> T getT(Function<Transaction, T> action, GraphDatabaseService currentDb) {
        try (Transaction tx = currentDb.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T todoThisDb(GraphDatabaseService db, String featureName, Function<Transaction, T> action) {
        final GraphDatabaseService currentDb = isCurrentDb(db, featureName)
                ? db : apocConfig().getSystemDb();

        return getT(action, currentDb);
//        try (Transaction tx = currentDb.beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
    }

//    private static  <T> T withSystemDb(Function<Transaction, T> action) {
//        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
//    }
}
