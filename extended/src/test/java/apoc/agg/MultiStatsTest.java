package apoc.agg;

import apoc.map.Maps;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
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
        
        db.executeTransactionally("""
                CREATE (n:Person {
                    louvain: 596,
                    neo4jImportId: "18349390",
                    wcc: 48,
                    lpa: 598,
                    name: "aaa",
                    wcc_cypher: 548
                });""");
        db.executeTransactionally("""
                CREATE (n:Person {
                    louvain: 596,
                    neo4jImportId: "18349390",
                    wcc: 48,
                    lpa: 598,
                    name: "eee",
                    wcc_cypher: 549
                });""");
        db.executeTransactionally("""
                CREATE (n:Person {
                    louvain: 596,
                    neo4jImportId: "18349390",
                    wcc: 47,
                    lpa: 596,
                    name: "iii",
                    wcc_cypher: 549
                })""");
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

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void test() {
        String s = db.executeTransactionally("""
                        MATCH (p:Person)
                        WITH p
                        CALL {
                            WITH p
                            MATCH (l:Person {louvain: p.louvain})
                            RETURN count(*) AS louvain
                        }
                        CALL {
                            WITH p
                            MATCH (wcc:Person {wcc: p.wcc})
                            RETURN count(*) AS wcc
                        }
                        CALL {
                            WITH p
                            MATCH (wcc_cypher:Person {wcc_cypher: p.wcc_cypher})
                            RETURN count(*) AS wcc_cypher
                        }
                        CALL {
                            WITH p
                            MATCH (lpa:Person {lpa: p.lpa})
                            RETURN count(*) AS lpa
                        }
                        RETURN p.name, lpa,  wcc, wcc_cypher, louvain
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
                RETURN apoc.agg.multiStats(p, ["lpa","wcc","louvain"], ["count"]) as data
                match (p:Person)
                return p.name, data.wcc.[toString(p.wcc)].count as size
                //MATCH (p:Person)
                //RETURN p.name, data.lpa.count as lpaSize, data.wcc.count as wccSize, data.louvain.count as louvainSize
                """, Map.of(), Result::resultAsString);
        System.out.println("s2 = " + s2);
        
        // todo...
    }
}
