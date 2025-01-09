//package apoc.agg;
//
//import apoc.Extended;
//import apoc.result.RowResult;
//import apoc.util.ExtendedListUtils;
//import apoc.util.Util;
//import org.neo4j.graphdb.Entity;
//import org.neo4j.logging.Log;
//import org.neo4j.procedure.Context;
//import org.neo4j.procedure.Description;
//import org.neo4j.procedure.Name;
//import org.neo4j.procedure.Procedure;
//import org.neo4j.procedure.UserAggregationFunction;
//import org.neo4j.procedure.UserAggregationResult;
//import org.neo4j.procedure.UserAggregationUpdate;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Stream;
//
//import static apoc.agg.AggregationUtil.updateAggregationValues;
//import static apoc.load.Jdbc.executeUpdate;
//
//
//// TODO TODO - spostarlo, non in agg, ma in jdbc
//
//@Extended
//public class Analytics {
//    
//    @Procedure
//    @Description("apoc.agg.rollup(<ANY>, [groupKeys], [aggKeys])" +
//                 "\n Emulate an Oracle/Mysql rollup command: `ROLLUP groupKeys, SUM(aggKey1), AVG(aggKey1), COUNT(aggKey1), SUM(aggKey2), AVG(aggKey2), ... `")
//    public Stream<RowResult> aggregate(
//            @Name("neo4jQuery") String neo4jQuery,
//            @Name("jdbc") String urlOrKey,
//            @Name("sqlQuery") String sqlQuery,
//            @Name(value = "params", defaultValue = "[]") List<Object> params,
//            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
//        
//        // TODO - scrivere sulla PR: add handling: some SQL database like Microsoft SQL Server create temp table in a different way
//        //      e.g. CREATE TABLE #table_name (column_name datatype);
//        //      document it
//
//        // step 1: temp table creation from neo4jQuery
//        
//        // step 2: 
//        executeUpdate(urlOrKey, sqlQuery, config, log, params);
//        
//        // step 3: return result
//    }
//    
//    
//    /* TODO 
//    MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
//    RETURN 
//        p.name AS actor, 
//        m.genre AS genre, 
//        r.roles AS roles, 
//        COUNT(m) AS movies_count
//
//    TODO - meglio non aggregation, così è più personalizzabile, posso scegliere quali risultati ottenere e come ottenerli 
//        altrimenti per fare qualcosa come sopra, con movies_count dovrei mettere un parametri aggKeys e fare cose strane
//     */
//    
//    
//    @Context
//    public Log log;
//    
//    @UserAggregationFunction("apoc.agg.analytics")
//    // TODO - descriptuin
//    @Description("apoc.agg.rollup(<ANY>, [groupKeys], [aggKeys])" +
//                 "\n Emulate an Oracle/Mysql rollup command: `ROLLUP groupKeys, SUM(aggKey1), AVG(aggKey1), COUNT(aggKey1), SUM(aggKey2), AVG(aggKey2), ... `")
//    public AnalyticsFunction rollup() {
//        return new AnalyticsFunction();
//    }
//
//    public static class AnalyticsFunction {
//
//        private final Map<List<Object>, Map<String, Number>> rolledUpData = new HashMap<>();
//        private List<String> groupKeysRes = null;
//
//        @UserAggregationUpdate
//        public void aggregate(
//                @Name("jdbc") String urlOrKey,
//                @Name("query") String query,
//                @Name(value = "params", defaultValue = "[]") List<Object> params, 
//                @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
//            
//            executeUpdate(urlOrKey, query, config, log, params);
//        }
//
//        private void rollupAggregationProperties(List<String> aggKeys, Entity entity, List<Object> partialKey) {
//            Map<String, Number> partialResult = rolledUpData.get(partialKey);
//            for(var aggKey: aggKeys) {
//                if (!entity.hasProperty(aggKey)) {
//                    continue;
//                }
//
//                Object property = entity.getProperty(aggKey);
//
//                String countKey = "COUNT(%s)".formatted(aggKey);
//                String sumKey = "SUM(%s)".formatted(aggKey);
//                String avgKey = "AVG(%s)".formatted(aggKey);
//
//                updateAggregationValues(partialResult, property, countKey, sumKey, avgKey);
//            }
//        }
//
//        /**
//         * Transform a Map.of(ListGroupKeys, MapOfAggResults) in a List of Map.of(AggResult + ListGroupKeyToMap)
//         */
//        @UserAggregationResult
//        public Object result() {
//            List<HashMap<String, Object>> list = rolledUpData.entrySet().stream()
//                    .map(e -> {
//                        HashMap<String, Object> map = new HashMap<>();
//                        for (int i = 0; i < groupKeysRes.size(); i++) {
//                            map.put(groupKeysRes.get(i), e.getKey().get(i));
//                        }
//                        map.putAll(e.getValue());
//                        return map;
//                    })
//                    .sorted((m1, m2) -> {
//                        for (String key : groupKeysRes) {
//                            Object value1 = m1.get(key);
//                            Object value2 = m2.get(key);
//                            int cmp = compareValues(value1, value2);
//                            if (cmp != 0) {
//                                return cmp;
//                            }
//                        }
//                        return 0;
//                    })
//                    .toList();
//
//            return list;
//        }
//
//        /**
//         * We use this instead of e.g. apoc.coll.sortMulti 
//         * since we have to handle the NULL_ROLLUP values as well
//         */
//        private static int compareValues(Object value1, Object value2) {
//            if (value1 == null && value2 == null) {
//                return 0;
//            } else if (value1 == null) {
//                return 1;
//            } else if (value2 == null) {
//                return -1;
//            } else if (NULL_ROLLUP.equals(value1) && NULL_ROLLUP.equals(value2)) {
//                return 0;
//            } else if (NULL_ROLLUP.equals(value1)) {
//                return 1;
//            } else if (NULL_ROLLUP.equals(value2)) {
//                return -1;
//            } else if (value1 instanceof Comparable && value2 instanceof Comparable) {
//                try {
//                    return ((Comparable<Object>) value1).compareTo(value2);
//                } catch (Exception e) {
//                    // e.g. different data types, like int and strings
//                    return 0;
//                }
//
//            } else {
//                return 0;
//            }
//        }
//    }
//}
