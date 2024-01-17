package apoc.agg;

import apoc.map.Maps;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

public class MultiStatsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, MaxAndMinItems.class, Maps.class);
        
        db.executeTransactionally("""
                CREATE (n:Person {
                    louvain: 596,
                    neo4jImportId: "18349390",
                    wcc: 48,
                    lpa: 598,
                    naam: "Jansen, J",
                    wcc_cypher: 548
                });""");
        db.executeTransactionally("""
                CREATE (n:Person {
                    louvain: 596,
                    neo4jImportId: "18349390",
                    wcc: 48,
                    lpa: 598,
                    naam: "Jansen, J",
                    wcc_cypher: 549
                });""");
        db.executeTransactionally("""
                CREATE (n:Person {
                    louvain: 596,
                    neo4jImportId: "18349390",
                    wcc: 47,
                    lpa: 596,
                    naam: "Jansen, J",
                    wcc_cypher: 549
                })""");
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
                        RETURN p.naam, lpa,  wcc, wcc_cypher, louvain
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
                        RETURN p.naam, map_lpa[toString(p.lpa)] as lpaSize,  map_wcc[toString(p.wcc)] as wccSize, map_louvain[toString(p.louvain)] as wccLouvain""",
                Map.of(), Result::resultAsString);
        System.out.println("s1 = " + s1);


        String s2 = db.executeTransactionally("""
                match (p:Person)
                with apoc.agg.multiStats(p, ["wcc","lpa","louvain"]) as data
                return p.name, data[toString(p.wcc)].count as size
                """, Map.of(), Result::resultAsString);
        System.out.println("s2 = " + s2);
    }
}
