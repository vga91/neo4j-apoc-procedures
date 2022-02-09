package apoc.load;

import apoc.util.Util;

import java.time.Instant;
import java.util.Calendar;
import java.util.Map;

import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractJdbcTest {

    protected static java.sql.Date hireDate = new java.sql.Date(new Calendar.Builder().setDate(2017, 04, 25).build().getTimeInMillis());

    protected static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from(Instant.parse("2016-06-22T17:10:25Z"));

    protected static java.sql.Time time = java.sql.Time.valueOf("15:37:00");

    public void assertResult(Map<String, Object> row) {
        assertResult(row, false, false, false);
    }

    public void assertResult(Map<String, Object> row, boolean isConverted, boolean isIgnored) {
        assertResult(row, isConverted, isIgnored, false);
    }
    
    public void assertResult(Map<String, Object> row, boolean isConverted, boolean isIgnored, boolean withUuid) {
        Map<String, Object> expected = Util.map( "SURNAME", null, "HIRE_DATE", hireDate.toLocalDate(), "EFFECTIVE_FROM_DATE",
                effectiveFromDate.toLocalDateTime(), "TEST_TIME", time.toLocalTime(), "NULL_DATE", null, "SMALL_NUM", isConverted ? 12345L : "12345", "BIG_NUM", "10223372036854776000.0");
        if (!isIgnored) {
            expected.put("NAME", "John");
        }
        final Map<String, Object> actual = (Map) row.get("row");
        if (withUuid) {
            // we check that is an uuid string and remove from final assertEquals(expected, actual)
            final String uuid = (String) actual.remove("UUID");
            assertTrue(uuid.matches(UUID_TEST_REGEXP));
        }
        assertEquals(expected, actual);
    }
}
