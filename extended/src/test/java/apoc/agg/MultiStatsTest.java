package apoc.agg;

import apoc.map.Maps;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.util.TestUtil.firstColumn;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiStatsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Maps.class, MultiStats.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }
    
    @After
    public void after() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void test2() {
        db.executeTransactionally("CREATE (:Test {a: 1, b: 2.0, c: 3.0}), " +
                                  "(:Test {a: 2, b: 2.0, c: 3.5}), " +
                                  "(:Test {a: 3, b: 3, c: 4.5})");

        Map<String, Object> stringObjectMap1 = db.executeTransactionally(
                "MATCH (n:Test) WITH n.a as prop return sum(prop) AS sum, avg(prop) AS avg, count(prop) AS count", Map.of(),
                Iterators::single);
        
        Map<String, Object> stringObjectMap2 = db.executeTransactionally(
                "MATCH (n:Test) WITH n.b as prop return sum(prop) AS sum, avg(prop) AS avg, count(prop) AS count", Map.of(),
                Iterators::single);
        
        Map<String, Object> stringObjectMap3 = db.executeTransactionally(
                "MATCH (n:Test) WITH n.c as prop return sum(prop) AS sum, avg(prop) AS avg, count(prop) AS count", Map.of(),
                Iterators::single);

        testCall(db, "MATCH (n:Test) RETURN apoc.agg.multiStats(n, ['a', 'b', 'c']) AS data", r -> {
            Map data = (Map) r.get("data");
            assertEquals(stringObjectMap1, data.get("a"));
            assertEquals(stringObjectMap2, data.get("b"));
            assertEquals(stringObjectMap3, data.get("c"));
        });
    }

    @Test
    public void test123123123() {
        db.executeTransactionally("""
                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "aaa", another: 548}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349391", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349392", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 10}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 10})
                    """);


        db.executeTransactionally("""
                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "aaa", another: 548}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349391", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349392", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 10}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 10})
                    """);
//        db.executeTransactionally("""
//                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549});""");
//        db.executeTransactionally("""
//                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 47, lpa: 596, name: "iii", another: 549})""");



        String s = db.executeTransactionally("""
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
                        RETURN p.name, sumLouvain, avgLouvain, countLouvain, sumWcc, avgWcc, countWcc, sumAnother, avgAnother, countAnother, sumLpa, avgLpa, countLpa
                        """, Map.of(),
                Result::resultAsString);
        System.out.println("s = " + s);


        String s1 = db.executeTransactionally("""
                        MATCH (p:Person)
                        WITH p.lpa as lpa, count(*) as sizeLpa
                        WITH apoc.map.fromPairs(collect([toString(lpa), sizeLpa])) as map_lpa
                        MATCH (p:Person)
                        WITH map_lpa, p.wcc as wcc, count(*) as sizeWcc
                        WITH map_lpa, apoc.map.fromPairs(collect([toString(wcc), sizeWcc])) as map_wcc
                        MATCH (p:Person)
                        WITH map_lpa, map_wcc, p.louvain as louvain, count(*) as sizeLouvain
                        WITH map_lpa, map_wcc, apoc.map.fromPairs(collect([toString(louvain), sizeLouvain])) as map_louvain
                        MATCH (p:Person)
                        RETURN p.name, map_lpa[toString(p.lpa)] as lpaSize,  map_wcc[toString(p.wcc)] as wccSize, map_louvain[toString(p.louvain)] as louvainSize""",
                Map.of(), Result::resultAsString);
        System.out.println("s1 = " + s1);


//        String s2 = db.executeTransactionally("""
//                match (p:Person)
//                WITH apoc.agg.multiStats(p, ["lpa","wcc","louvain"], ["count"]) as data
//                MATCH (p:Person)
//                RETURN p.name, data.lpa.count as lpaSize, data.wcc.count as wccSize, data.louvain.count as louvainSize
//                """, Map.of(), Result::resultAsString);
//        System.out.println("s2 = " + s2);
        String s2 = db.executeTransactionally("""
                match (p:Person)
                with apoc.agg.multiStats(p, ["lpa","wcc","louvain"], ["count"]) as data
                match (p:Person)
                return p.name, data.wcc[toString(p.wcc)], data.louvain[toString(p.louvain)], data.lpa[toString(p.lpa)]
                //MATCH (p:Person)
                //RETURN p.name, data.lpa.count as lpaSize, data.wcc.count as wccSize, data.louvain.count as louvainSize
                """, Map.of(), Result::resultAsString);
        System.out.println("s2 = " + s2);
        
    }

    // similar to https://community.neo4j.com/t/listing-the-community-size-of-different-community-detection-algorithms-already-calculated/42895
    @Test
    public void test() {
        db.executeTransactionally("""
                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "aaa", another: 548}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349391", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349392", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 10}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 10})
                    """);
//        db.executeTransactionally("""
//                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549});""");
//        db.executeTransactionally("""
//                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 47, lpa: 596, name: "iii", another: 549})""");
        
        
        
        String s = db.executeTransactionally("""
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
                        RETURN p.name, sumLouvain, avgLouvain, countLouvain, sumWcc, avgWcc, countWcc, sumAnother, avgAnother, countAnother, sumLpa, avgLpa, countLpa
                        """, Map.of(),
                Result::resultAsString);
        System.out.println("s = " + s);


        String s1 = db.executeTransactionally("""
                        MATCH (p:Person)
                        WITH p.lpa as lpa, count(*) as sizeLpa
                        WITH apoc.map.fromPairs(collect([toString(lpa), sizeLpa])) as map_lpa
                        MATCH (p:Person)
                        WITH map_lpa, p.wcc as wcc, count(*) as sizeWcc
                        WITH map_lpa, apoc.map.fromPairs(collect([toString(wcc), sizeWcc])) as map_wcc
                        MATCH (p:Person)
                        WITH map_lpa, map_wcc, p.louvain as louvain, count(*) as sizeLouvain
                        WITH map_lpa, map_wcc, apoc.map.fromPairs(collect([toString(louvain), sizeLouvain])) as map_louvain
                        MATCH (p:Person)
                        RETURN p.name, map_lpa[toString(p.lpa)] as lpaSize,  map_wcc[toString(p.wcc)] as wccSize, map_louvain[toString(p.louvain)] as louvainSize""",
                Map.of(), Result::resultAsString);
        System.out.println("s1 = " + s1);


//        String s2 = db.executeTransactionally("""
//                match (p:Person)
//                WITH apoc.agg.multiStats(p, ["lpa","wcc","louvain"], ["count"]) as data
//                MATCH (p:Person)
//                RETURN p.name, data.lpa.count as lpaSize, data.wcc.count as wccSize, data.louvain.count as louvainSize
//                """, Map.of(), Result::resultAsString);
//        System.out.println("s2 = " + s2);
        String s2 = db.executeTransactionally("""
                match (p:Person)
                with apoc.agg.multiStats(p, ["lpa","wcc","louvain"], ["count"]) as data
                match (p:Person)
                return p.name, data.wcc[toString(p.wcc)], data.louvain[toString(p.louvain)], data.lpa[toString(p.lpa)]
                //MATCH (p:Person)
                //RETURN p.name, data.lpa.count as lpaSize, data.wcc.count as wccSize, data.louvain.count as louvainSize
                """, Map.of(), Result::resultAsString);
        System.out.println("s2 = " + s2);
        
        // todo...
    }


    // similar to https://community.neo4j.com/t/how-could-to-replace-groupe-by-roll-up-and-group-by-cube-in-cypher-aggregation/44762
    @Test
    public void test23() {
        db.executeTransactionally("""
                CREATE (:Sales {ProductID: 10, CustomerID: 48, salesAmount: 100}),
                    (:Sales {ProductID: 90, CustomerID: 38, salesAmount: 200}),
                    (:Sales {ProductID: 90, CustomerID: 38, salesAmount: 300}),
                    (:Sales {ProductID: 90, CustomerID: 108, salesAmount: 300}),
                    (:Sales {ProductID: 90, CustomerID: 108, salesAmount: 300}),
                    (:Sales {ProductID: 20, CustomerID: 108, salesAmount: 300}),
                    (:Sales {ProductID: 20, CustomerID: 108, salesAmount: 300}),
                    (:Sales {ProductID: 20, CustomerID: 108, salesAmount: 301}),
                    (:Sales {ProductID: 20, CustomerID: 108, salesAmount: 301}),
                    (:Sales {ProductID: 30, CustomerID: 108, salesAmount: 300}),
                    (:Sales {ProductID: 30, CustomerID: 108, salesAmount: 301}),
                    (:Sales {ProductID: 30, CustomerID: 108, salesAmount: 301}),
                    (:Sales {ProductID: 30, CustomerID: 118, salesAmount: 300}),
                    (:Sales {ProductID: 30, CustomerID: 118, salesAmount: 301}),
                    (:Sales {ProductID: 40, CustomerID: 28, salesAmount: 400})""");


//        String s1 = db.executeTransactionally("""
//                        MATCH (s:Sales)
//                        WITH s.ProductID as pid, s.CustomerID as cid, sum(s.salesAmount) as sa
//                        CALL {
//                           WITH pid, cid, sa
//                           WITH pid, cid, sum(sa) as saAmount RETURN saAmount
//                        }
//                        CALL {
//                           WITH pid, cid, sa
//                           WITH cid, sa, sum(pid) as saProduct RETURN saProduct
//                        }
//                        RETURN pid, cid, saProduct, saAmount, sa""",
//                Map.of(), Result::resultAsString);
        String s1 = db.executeTransactionally("""
                        MATCH (p:Sales)
                        WITH p
                        CALL {
                            WITH p
                            MATCH (l:Sales {ProductID: p.ProductID})
                            RETURN sum(l.ProductID) AS louvain
                        }
                        CALL {
                            WITH p
                            MATCH (another:Sales {salesAmount: p.salesAmount})
                            RETURN sum(another.salesAmount) AS another
                        }
                        RETURN p.CustomerID, another, louvain""",
                Map.of(), Result::resultAsString);
        System.out.println("s1 = " + s1);
        
        
//        String s11 = db.executeTransactionally("""
//                        MATCH (s:Sales)
//                        WITH s.CustomerID as cid, sum(s.ProductID) as pid, sum(s.salesAmount) as sum
//                        RETURN cid, pid, sum
//                     //   UNION
//                     //   MATCH (s:Sales)
//                     //   WITH null as cid, null as pid, sum(s.salesAmount) as sum
//                     //   RETURN cid, pid, sum
//                     //   UNION
//                     //   MATCH (s:Sales)
//                     //   WITH s.CustomerID as cid, null as pid, sum(s.salesAmount) as sum
//                     //   RETURN cid, pid, sum;""",
//                Map.of(), Result::resultAsString);
//        System.out.println("s11 = " + s11);


        String s2 = db.executeTransactionally("""
                match (p:Sales)
                WITH apoc.agg.multiStats(p, ["ProductID","salesAmount"], ["sum"]) as data
                match (p:Sales)
                RETURN p.CustomerID, data.ProductID[toString(p.ProductID)].sum, data.salesAmount[toString(p.salesAmount)].sum
                """, Map.of(), Result::resultAsString);
        System.out.println("s2 = " + s2);
        
        String s23 = db.executeTransactionally("""
                match (p:Sales)
                RETURN apoc.agg.multiStats(p, ["ProductID","salesAmount"], ["sum"]) as data
                // match (p:Sales)
                // RETURN DISTINCT p.CustomerID, data.ProductID[toString(p.ProductID)].sum, data.salesAmount[toString(p.salesAmount)].sum
                """, Map.of(), Result::resultAsString);
        System.out.println("s23 = " + s23);
        
    }
     
}
