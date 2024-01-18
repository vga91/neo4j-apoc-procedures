package apoc.util;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExtendedTestUtil {
    
    public static void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertEquals(expected.keySet(), actual.keySet());
            
            actual.forEach((key, value) -> {
                if (value instanceof Map mapVal) {
                    assertMapEquals((Map<String, Object>) expected.get(key), mapVal);
                } else {
                    assertEquals(expected.get(key), value);
                }
            });
        }
    }
}
