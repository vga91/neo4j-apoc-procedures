package apoc.vectordb;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VectorDbTestUtil {

    public static void assertBerlinVector(Map row) {
        assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
        assertEquals("1", row.get("id").toString());
    }

    public static void assertLondonVector(Map row) {
        assertEquals(Map.of("city", "London", "foo", "two"), row.get("metadata"));
        assertEquals("2", row.get("id").toString());
    }


    public static void vectorEntityAssertions(Result r, boolean isNew) {
        ResourceIterator<Map> props = r.columnAs("props");
        Map next = props.next();
        assertEquals("Berlin", next.get("city"));
        if (!isNew) {
            assertEquals("one", next.get("myId"));
        }
        assertTrue(next.get("vect") instanceof float[]);
        next = props.next();
        assertEquals("London", next.get("city"));
        if (!isNew) {
            assertEquals("two", next.get("myId"));
        }
        assertTrue(next.get("vect") instanceof float[]);

        assertFalse(props.hasNext());
    }
}
