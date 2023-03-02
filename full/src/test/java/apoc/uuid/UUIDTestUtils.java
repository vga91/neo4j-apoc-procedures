package apoc.uuid;

import org.hamcrest.Matchers;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.testCallEventually;
import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static apoc.uuid.UuidConfig.*;
import static apoc.uuid.UuidConfig.ADD_TO_SET_LABELS_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class UUIDTestUtils {

    // todo - uuid test utils
    public static void awaitUuidDiscovered(GraphDatabaseService db, String label) {
        awaitUuidDiscovered(db, label, DEFAULT_UUID_PROPERTY, DEFAULT_ADD_TO_SET_LABELS);
    }
    public static void awaitUuidDiscovered(GraphDatabaseService db, String label, String expectedUuidProp, boolean expectedAddToSetLabels) {
        String call = "CALL apoc.uuid.list() YIELD properties, label WHERE label = $label " +
                "RETURN properties.uuidProperty AS uuidProperty, properties.addToSetLabels AS addToSetLabels";
        testCallEventually(db, call,
                Map.of("label", label),
                row -> {
                    assertEquals(expectedUuidProp, row.get(UUID_PROPERTY_KEY));
                    assertEquals(expectedAddToSetLabels, row.get(ADD_TO_SET_LABELS_KEY));
                }, TIMEOUT);
    }

    public static void assertIsUUID(String uuid) {
        assertThat(uuid, Matchers.matchesRegex(UUID_TEST_REGEXP));
    }
}
