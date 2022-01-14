package apoc.kernel;

import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.neo4j.test.assertion.Assert.assertEventually;

public class KernelTestUtils {

    public static void checkStatusDetails(GraphDatabaseService db, String query, Map<String, Object> params) {
        checkStatusDetails(db, query, params, null);
    }
    public static void checkStatusDetails(GraphDatabaseService db, String query, Map<String, Object> params, String startQuery) {
//        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Thread thread = new Thread(() -> db.executeTransactionally(query, params, Result::resultAsString));
        thread.setDaemon(true);
        thread.start();
//        final Future<String> xsubmit = executor.submit(() -> db.executeTransactionally(query, params, Result::resultAsString));
// todo - provare a cambiare... facendo EQUALS!! 
        
//        if (startQuery == null) {
//            startQuery = query;
//        }
        
//        try {
//            submit.get();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
        
        String finalStartQuery = startQuery == null ? query : startQuery;
        assertEventually(() -> TestUtil.<String>singleResultFirstColumn(db,
//                "CALL dbms.listTransactions() yield statusDetails, currentQuery where not currentQuery STARTS WITH 'CALL dbms' RETURN statusDetails", 
                "CALL dbms.listTransactions() yield statusDetails, currentQuery where currentQuery STARTS WITH $startQuery RETURN statusDetails", 
                Map.of("startQuery", finalStartQuery)),
//                StringUtils::isNotEmpty, 
                (value) -> {
                    System.out.println("status");
                    System.out.println(value);
                    return StringUtils.isNotEmpty(value);
                },
                15L, TimeUnit.SECONDS);
        try {
            thread.join(30);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }   
        
//        try {
//            executor.awaitTermination(30, TimeUnit.SECONDS);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }   
    }
}
