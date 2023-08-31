package apoc;

import org.junit.Test;
import org.neo4j.procedure.Mode;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.QueryExecutionType.QueryType;

public class VersionUpdateChecksTest {

    /**
     * If any new org.neo4j.procedure.Mode or org.neo4j.graphdb.QueryExecutionType are added, this test will fail.
     * Add the new stuff to the tests as well
     * and update validateProcedure in {@link apoc.custom.CypherProcedures} to work with them.
     */
    @Test
    public void testAPOCisAwareOfAllModes() {
        List<Mode> expected = List.of(
                Mode.READ,
                Mode.WRITE,
                Mode.SCHEMA,
                Mode.DBMS,
                Mode.DEFAULT
        );
        List<Mode> actual = Arrays.asList(Mode.values());
        assertEquals(expected, actual);
    }

    @Test
    public void testAPOCisAwareOfAllQueryExecutionTypes() {
        List<QueryType> expected = List.of(
                QueryType.READ_ONLY,
                QueryType.READ_WRITE,
                QueryType.WRITE,
                QueryType.SCHEMA_WRITE,
                QueryType.DBMS
        );
        List<QueryType> actual = Arrays.asList(QueryType.values());
        assertEquals(expected, actual);
    }
}
