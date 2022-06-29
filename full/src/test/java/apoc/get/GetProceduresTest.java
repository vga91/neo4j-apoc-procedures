package apoc.get;

import apoc.es.ElasticSearch;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author mh
 * @since 16.04.16
 */
public class GetProceduresTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, GetProcedures.class, ElasticSearch.class);
    }

    @Test
    public void shouldWriteSafeStrings() {
        TestUtil.testCall(db, "CALL apoc.es.stats($host)", Map.of("host", "http://localhost:9200"), r -> {
            System.out.println("GetProceduresTest.shouldWriteSafeStrings-------");
            System.out.println(r.get("value"));

//            Object numOfDocs = extractValueFromResponse(r, "$._all.total.docs.count");
//            assertNotEquals(0, numOfDocs);
        });
    }

    @Test
    public void testNodes() throws Exception {
        List<Long> ids = TestUtil.firstColumn(db, "UNWIND range(0,2) as id CREATE (n:Node {id:id}) return id(n) as id");
        TestUtil.testResult(db, "CALL apoc.get.nodes($ids)", map("ids",ids), r -> {
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
        });
    }

    @Test
    public void testRels() throws Exception {
        List<Long> ids = TestUtil.firstColumn(db, "CREATE (n) WITH n UNWIND range(0,2) as id CREATE (n)-[r:KNOWS]->(n) return id(r) as id");
        TestUtil.testResult(db, "CALL apoc.get.rels($ids)", map("ids",ids), r -> {
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
        });
    }

    @Test
    public void testArrayOfIds() {
        String query = "MERGE (g:Foo {id: 1})\n" +
                "MERGE (h:Foo {id: 2})\n" +
                "MERGE (g)-[r:BAR]->(h)\n" +
                "MERGE (t:Temp)\n" +
                "SET t.ids = [] + ID(r)\n" +
                "WITH t\n" +
                "CALL apoc.get.rels(t.ids) YIELD rel\n" +
                "RETURN rel";
        TestUtil.testResult(db, query, Collections.emptyMap(), r -> {
            final ResourceIterator<Relationship> relIT = r.columnAs("rel");
            final Relationship rel = relIT.next();
            assertEquals("BAR", rel.getType().name());
            assertFalse(r.hasNext());
        });
    }
}
