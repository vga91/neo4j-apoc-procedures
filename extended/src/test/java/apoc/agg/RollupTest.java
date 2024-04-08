package apoc.agg;

import apoc.map.Maps;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;


public class RollupTest {
    /**
     * TODO: METTERE ESEMPIO CON ROLLUP USANDO MYSQL
     * 
     * 
     * +------------+------------+-------+----------------------+
     * | SupplierID | CategoryID | Price | Unit                 |
     * +------------+------------+-------+----------------------+
     * |          1 |          1 |    18 | 10 boxes x 20 bags   |
     * |          1 |          1 |    19 | 24 - 12 oz bottles   |
     * |          1 |          2 |    10 | 12 - 550 ml bottles  |
     * |          2 |          2 |    22 | 48 - 6 oz jars       |
     * |          2 |          2 |    21 | 36 boxes             |
     * |          3 |          2 |    25 | 12 - 8 oz jars       |
     * |          3 |          7 |    30 | 12 - 1 lb pkgs.      |
     * |          3 |          2 |    40 | 12 - 12 oz jars      |
     * |          4 |          6 |    97 | 18 - 500 g pkgs.     |
     * |          4 |          8 |    31 | 12 - 200 ml jars     |
     * |          5 |          4 |    21 | 1 kg pkg.            |
     * |          5 |          4 |    38 | 10 - 500 g pkgs.     |
     * |          6 |          8 |     6 | 2 kg box             |
     * |          6 |          7 |    23 | 40 - 100 g pkgs.     |
     * |          6 |          2 |    16 | 24 - 250 ml bottles  |
     * |          7 |          3 |    17 | 32 - 500 g boxes     |
     * |          7 |          6 |    39 | 20 - 1 kg tins       |
     * |          7 |          8 |    63 | 16 kg pkg.           |
     * |          8 |          3 |     9 | 10 boxes x 12 pieces |
     * |          8 |          3 |    81 | 30 gift boxes        |
     * |          8 |          3 |    10 | 24 pkgs. x 4 pieces  |
     * |          9 |          5 |    21 | 24 - 500 g pkgs.     |
     * |          9 |          5 |     9 | 12 - 250 g pkgs.     |
     * |         10 |          1 |     5 | 12 - 355 ml cans     |
     * |         11 |          3 |    14 | 20 - 450 g glasses   |
     * |         11 |          3 |    31 | 100 - 250 g bags     |
     * |         11 |          3 |    44 | 100 - 100 g pieces   |
     * |         12 |          7 |    46 | 25 - 825 g cans      |
     * |         12 |          6 |   124 | 50 bags x 30 sausgs. |
     * |         13 |          8 |    26 | 10 - 200 g glasses   |
     * |         14 |          4 |    13 | 12 - 100 g pkgs      |
     * |         14 |          4 |    32 | 24 - 200 g pkgs.     |
     * |         15 |          4 |     3 | 500 g                |
     * |         16 |          1 |    14 | 24 - 12 oz bottles   |
     * |         16 |          1 |    18 | 24 - 12 oz bottles   |
     * |         17 |          8 |    19 | 24 - 250 g jars      |
     * |         17 |          8 |    26 | 12 - 500 g pkgs.     |
     * |         18 |          1 |   264 | 12 - 75 cl bottles   |
     * |         18 |          1 |    18 | 750 cc per bottle    |
     * |         19 |          8 |    18 | 24 - 4 oz tins       |
     * |         19 |          8 |    10 | 12 - 12 oz cans      |
     * |         20 |          5 |    14 | 32 - 1 kg pkgs.      |
     * |         20 |          1 |    46 | 16 - 500 g tins      |
     * |         20 |          2 |    19 | 20 - 2 kg bags       |
     * |         21 |          8 |    10 | 1k pkg.              |
     * |         21 |          8 |    12 | 4 - 450 g glasses    |
     * |         22 |          3 |    10 | 10 - 4 oz boxes      |
     * |         22 |          3 |    13 | 10 pkgs.             |
     * |         23 |          3 |    20 | 24 - 50 g pkgs.      |
     * |         23 |          3 |    16 | 12 - 100 g bars      |
     * |         24 |          7 |    53 | 50 - 300 g pkgs.     |
     * |         24 |          5 |     7 | 16 - 2 kg boxes      |
     * |         24 |          6 |    33 | 48 pieces            |
     * |         25 |          6 |     7 | 16 pies              |
     * |         25 |          6 |    24 | 24 boxes x 2 pies    |
     * |         26 |          5 |    38 | 24 - 250 g pkgs.     |
     * |         26 |          5 |    20 | 24 - 250 g pkgs.     |
     * |         27 |          8 |    13 | 24 pieces            |
     * |         28 |          4 |    55 | 5 kg pkg.            |
     * |         28 |          4 |    34 | 15 - 300 g rounds    |
     * |         29 |          2 |    29 | 24 - 500 ml bottles  |
     * |         29 |          3 |    49 | 48 pies              |
     * |          7 |          2 |    44 | 15 - 625 g jars      |
     * |         12 |          5 |    33 | 20 bags x 4 pieces   |
     * |          2 |          2 |    21 | 32 - 8 oz bottles    |
     * |          2 |          2 |    17 | 24 - 8 oz jars       |
     * |         16 |          1 |    14 | 24 - 12 oz bottles   |
     * |          8 |          3 |    13 | 10 boxes x 8 pieces  |
     * |         15 |          4 |    36 | 10 kg pkg.           |
     * |          7 |          1 |    15 | 24 - 355 ml bottles  |
     * |         15 |          4 |    22 | 10 - 500 g pkgs.     |
     * |         14 |          4 |    35 | 24 - 200 g pkgs.     |
     * |         17 |          8 |    15 | 24 - 150 g jars      |
     * |          4 |          7 |    10 | 5 kg pkg.            |
     * |         12 |          1 |     8 | 24 - 0.5 l bottles   |
     * |         23 |          1 |    18 | 500 ml               |
     * |         12 |          2 |    13 | 12 boxes             |
     * |          1 |       NULL |    18 | 10 boxes x 20 bags   |
     * |       NULL |       NULL |    18 | 10 boxes x 20 bags   |
     * +------------+------------+-------+----------------------+
     * 
     * 
     * 
     */

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Maps.class, Rollup.class);

        db.executeTransactionally("""
                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "aaa", another: 548}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349391", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349392", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 10}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 10})""");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    // similar to https://community.neo4j.com/t/listing-the-community-size-of-different-community-detection-algorithms-already-calculated/42895
    @Test
    public void testMultiStatsComparedWithCypherMultiAggregation() {
        String multiAggregationResult = db.executeTransactionally("""
                        MATCH (p:Person)
                        WITH p
                        CALL {
                            WITH p
                            MATCH (n:Person {louvain: p.louvain})
                            RETURN sum(p.louvain) AS sumLouvain, avg(p.louvain) AS avgLouvain, count(p.louvain) AS countLouvain
                        }
                        CALL {
                            WITH p
                            MATCH (n:Person {wcc: p.wcc})
                            RETURN sum(p.wcc) AS sumWcc, avg(p.wcc) AS avgWcc, count(p.wcc) AS countWcc
                        }
                        CALL {
                            WITH p
                            MATCH (n:Person {another: p.another})
                            RETURN sum(p.another) AS sumAnother, avg(p.another) AS avgAnother, count(p.another) AS countAnother
                        }
                        CALL {
                            WITH p
                            MATCH (lpa:Person {lpa: p.lpa})
                            RETURN sum(p.lpa) AS sumLpa, avg(p.lpa) AS avgLpa, count(p.lpa) AS countLpa
                        }
                        RETURN p.name,
                            sumLouvain, avgLouvain, countLouvain,
                            sumWcc, avgWcc, countWcc,
                            sumAnother, avgAnother, countAnother,
                            sumLpa, avgLpa, countLpa""", Map.of(),
                result -> result.resultAsString());

        /*
        [ {key1: val1, key2: val2, key2: val3, <AGGR>} ] 
         */
        
        
        /*
        - riga 1
        - riga 2
        
        ----
        
        - 
        
         */
        
        String multiStatsResult = db.executeTransactionally("""
                match (p:Person)
                with apoc.agg.rollup(p, ["lpa","wcc","louvain", "another"]) as data
                match (p:Person)
                return p.name,
                    data.wcc[toString(p.wcc)].avg AS avgWcc,
                    data.louvain[toString(p.louvain)].avg AS avgLouvain,
                    data.lpa[toString(p.lpa)].avg AS avgLpa,
                    data.another[toString(p.another)].avg AS avgAnother,
                    data.another[toString(p.another)].count AS countAnother,
                    data.wcc[toString(p.wcc)].count AS countWcc,
                    data.louvain[toString(p.louvain)].count AS countLouvain,
                    data.lpa[toString(p.lpa)].count AS countLpa,
                    data.another[toString(p.another)].sum AS sumAnother,
                    data.wcc[toString(p.wcc)].sum AS sumWcc,
                    data.louvain[toString(p.louvain)].sum AS sumLouvain,
                    data.lpa[toString(p.lpa)].sum AS sumLpa
                """, Map.of(), r -> r.resultAsString());

        System.out.println("multiStatsResult = \n" + multiStatsResult);
        assertEquals(multiAggregationResult, multiStatsResult);
        
    }
     
}
