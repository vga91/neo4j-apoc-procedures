package apoc.lock;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LockTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.lock_acquisition_timeout, Duration.ofSeconds(1));

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Lock.class);
    }

    @Test
    public void shouldReadLockBlockAWrite() throws Exception {

        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            final Node n = Iterators.single(tx.execute("match (n) CALL apoc.lock.read.nodes([n]) return n").columnAs("n"));
            assertEquals(n, node);

            final Thread thread = new Thread(() -> {
                System.out.println(Instant.now().toString() + " pre-delete");
                try {
                    db.executeTransactionally("match (n) delete n", Collections.emptyMap(), result -> result.resultAsString());
                    fail("expecting lock timeout");
                } catch (LockAcquisitionTimeoutException e) {
                }
                System.out.println(Instant.now().toString() + " delete");

            });
            thread.start();
            thread.join(5000L);

            // the blocked thread didn't do any work, so we still have nodes
            long count = Iterators.count(tx.execute("match (n) return n").columnAs("n"));
            assertEquals(1, count);

            tx.commit();
        }

    }

    @Test
    public void shouldBlockRelationshipsWithSuperNode() throws Exception {
        Node node;
        Node node2;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode(Label.label("One"));
            node2 = tx.createNode(Label.label("Two"));
            final Relationship newRel = node.createRelationshipTo(node2, RelationshipType.withName("NEW_REL"));
            newRel.setProperty("id", 1000);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            
            // we create a super node (:One)
            final long single = Iterators.single(db.executeTransactionally(
                    "UNWIND range(1, 499) as idx with idx MATCH (n:One) \n" +
                            "CREATE (n)-[:NEW_REL {id: idx}]->(:Other {id: idx}) RETURN count(n) as count", 
                    Collections.emptyMap(),
                    r -> r.columnAs("count")));
            assertEquals(499L, single);
            
            // block nodes and rel with prop id = 333
            final Node n = Iterators.single(tx.execute("MATCH (n:One) CALL apoc.lock.read.nodes([n]) return n").columnAs("n"));
            final Node n2 = Iterators.single(tx.execute("MATCH (n:Two) CALL apoc.lock.read.nodes([n]) return n").columnAs("n"));
            final Relationship relationship = Iterators.single(tx.execute("MATCH (:One)-[r:NEW_REL {id: 333}]->(e) with r CALL apoc.lock.read.rels([r]) return r").columnAs("r"));
            assertEquals(n, node);
            assertEquals(n2, node2);
            assertEquals(node, relationship.getStartNode());

            final String deleteQuery = "MATCH (p:One)-[rel:NEW_REL]->(e:Two) DELETE rel";
            checkLockForRelationships(tx, deleteQuery);

            final String mergeQuery = "MATCH (p:One), (e:Two) MERGE (p)-[rel:NEW_REL {id: 999}]->(e)";
            checkLockForRelationships(tx, mergeQuery);

            final String createQuery = "MATCH (p:One), (e:Two) CREATE (p)-[rel:NEW_REL {id: 888}]->(e)";
            checkLockForRelationships(tx, createQuery);

            final String deleteBlockedRelQuery = "MATCH (:One)-[rel:NEW_REL {id: 333}]->() DELETE rel";
            checkLockForRelationships(tx, deleteBlockedRelQuery);

            final String removePropBlockedRelQuery = "MATCH (:One)-[rel:NEW_REL {id: 333}]->(e) remove rel.id ";
            checkLockForRelationships(tx, removePropBlockedRelQuery);

            tx.commit();
        }
    }

    private void checkLockForRelationships(Transaction tx, String query) throws InterruptedException {
        final Thread thread = new Thread(() -> {
            try {
                db.executeTransactionally(query);
            } catch (LockAcquisitionTimeoutException ignored) {}
        });
        thread.start();
        thread.join(5000L);

        // the blocked thread didn't do any work, so we still have same rels
        long count = Iterators.count(tx.execute("match (:One)-[r:NEW_REL]->() WHERE r.id IS NOT NULL return r").columnAs("r"));
        assertEquals(500, count);
    }
}
