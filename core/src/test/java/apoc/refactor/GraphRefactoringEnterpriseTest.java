package apoc.refactor;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.value.NullValue;

import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class GraphRefactoringEnterpriseTest {
    private static final String CLONE_NODES_QUERY = "match (n:MyBook) with n call apoc.refactor.cloneNodes([n], true) " +
            "YIELD output, error RETURN output, error";
    private static final String CLONE_SUBGRAPH_QUERY = "MATCH (n:MyBook) with n call apoc.refactor.cloneSubgraph([n], [], {}) YIELD output, error RETURN output, error";
    private static final String EXTRACT_QUERY = "MATCH p=(:Start)-[r:TO_MOVE]->(:End) with r call apoc.refactor.extractNode([r], ['MyBook'], 'OUT', 'IN') " +
            "YIELD output, error RETURN output, error";
    private static final String CREATE_REL_FOR_EXTRACT_NODE = "CREATE (:Start)-[r:TO_MOVE {name: 'foobar', surname: 'baz'}]->(:End)";
    private static final String DELETE_REL_FOR_EXTRACT_NODE = "MATCH p=(:Start)-[r:TO_MOVE]->(:End) DELETE p";
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(true);
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            session.close();
            neo4jContainer.close();
        }
    }
    
    @Test
    public void testCloneNodesWithNodeKeyConstraint() {
        nodeKeyCommons(CLONE_NODES_QUERY);
    }

    @Test
    public void testCloneNodesWithBothExistenceAndUniqueConstraint() {
        uniqueNotNullCommons(CLONE_NODES_QUERY);
    }
    
    @Test
    public void testCloneSubgraphWithNodeKeyConstraint() {
        nodeKeyCommons(CLONE_SUBGRAPH_QUERY);
    }

    @Test
    public void testCloneSubgraphWithBothExistenceAndUniqueConstraint() {
        uniqueNotNullCommons(CLONE_SUBGRAPH_QUERY);
    }
    
    @Test
    public void testExtractNodesWithNodeKeyConstraint() {
        session.writeTransaction(tx -> tx.run(CREATE_REL_FOR_EXTRACT_NODE));
        nodeKeyCommons(EXTRACT_QUERY);
        session.writeTransaction(tx -> tx.run(DELETE_REL_FOR_EXTRACT_NODE));
    }

    @Test
    public void testExtractNodesWithBothExistenceAndUniqueConstraint() {
        session.writeTransaction(tx -> tx.run(CREATE_REL_FOR_EXTRACT_NODE));
        uniqueNotNullCommons(EXTRACT_QUERY);
        session.writeTransaction(tx -> tx.run(DELETE_REL_FOR_EXTRACT_NODE));
    }

    private void nodeKeyCommons(String query) {
        session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT nodeKey ON (p:MyBook) ASSERT (p.name, p.surname) IS NODE KEY"));
        cloneNodesAssertions(query, "already exists with label `MyBook` and properties `name` = 'foobar', `surname` = 'baz'");
        session.writeTransaction(tx -> tx.run("DROP CONSTRAINT nodeKey"));
        
    }

    private void uniqueNotNullCommons(String query) {
        session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT unique ON (p:MyBook) ASSERT (p.name) IS UNIQUE"));
        session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT notNull ON (p:MyBook) ASSERT (p.name) IS NOT NULL"));

        cloneNodesAssertions(query, "already exists with label `MyBook` and property `name` = 'foobar'");
        session.writeTransaction(tx -> tx.run("DROP CONSTRAINT unique"));
        session.writeTransaction(tx -> tx.run("DROP CONSTRAINT notNull"));
    }
    
    @Test
    public void issue2797WithCloneNodes() {
        extracted(CLONE_NODES_QUERY);
    }

    @Test
    public void issue2797WithExtractNode() {
        session.writeTransaction(tx -> tx.run("CREATE (:Start)-[r:TO_MOVE {name: 1}]->(:End)"));
        extracted(EXTRACT_QUERY);
    }

    @Test
    public void issue2797WithCloneSubgraph() {
        extracted(CLONE_SUBGRAPH_QUERY);
    }

    private void extracted(String extractQuery) {
        session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT unique_book ON (book:MyBook) ASSERT book.name IS UNIQUE"));

        session.writeTransaction(tx -> tx.run("CREATE (n:MyBook {name: 1})"));

        session.writeTransaction(tx -> {
            final Record record = tx.run(extractQuery).single();
            final String error = record.get("error").asString();
            assertTrue(error.contains("already exists with label `MyBook` and property `name` = 1"));
            assertEquals(NullValue.NULL, record.get("output"));
            return null;
        });

        session.readTransaction(tx -> {
            final Map<String, Object> bookProps = tx.run("MATCH (n:MyBook) RETURN properties(n) AS props").single().asMap();
            assertEquals(Map.of("name", 1L), bookProps.get("props"));
            return null;
        });
        session.writeTransaction(tx -> tx.run("DROP CONSTRAINT unique_book"));
        session.writeTransaction(tx -> tx.run("MATCH (n:MyBook) DELETE n"));
    }

    private void cloneNodesAssertions(String query, String message) {
        session.writeTransaction(tx -> tx.run("CREATE (n:MyBook {name: 'foobar', surname: 'baz'})"));
        testCall(session, query,
                r -> {
                    final String error = (String) r.get("error");
                    assertTrue(error.contains(message));
                    assertNull(r.get("output"));
                    
                });
        session.writeTransaction(tx -> tx.run("MATCH (n:MyBook) DELETE n"));
    }
}
