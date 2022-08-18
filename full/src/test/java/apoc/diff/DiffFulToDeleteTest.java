package apoc.diff;

import apoc.bolt.Bolt;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;
import java.util.Scanner;

public class DiffFulToDeleteTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    @Before
    public void before() throws Exception {
        TestUtil.registerProcedure(db, Bolt.class, DiffFull.class);
        
        try (Scanner scanner = new Scanner(Thread
                .currentThread()
                .getContextClassLoader()
                .getResourceAsStream("init_neo4j_diff.cypher"))
                .useDelimiter(";")) {
            while (scanner.hasNext()) {
                String statement = scanner.next().trim();
                if (statement.isEmpty()) {
                    continue;
                }
                db.executeTransactionally(statement);
            }
        }
    }

    @After
    public void after() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void shouldNotFindDifferencesInTheSameDbUsingDatabaseTypeAndFindById() {
        // with target type = "DATABASE"
        TestUtil.testCallEmpty(db, "CALL apoc.diff.graphs($querySourceDest, $querySourceDest, $conf)",
                Map.of("querySourceDest", "MATCH p = (start)-[rel:KNOWS]->(end) RETURN start, rel, end",
                        "conf", Map.of("source", Map.of(),
                                "dest", Map.of("target", Map.of("type", SourceDestConfig.SourceDestConfigType.DATABASE.name(), "value", "neo4j")),
                                "findById", true
                        )));
    }
}
