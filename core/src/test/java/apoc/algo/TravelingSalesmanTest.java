package apoc.algo;

import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.Utils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
//import static apoc.algo.TravellingSalesmanSolver.City;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class TravelingSalesmanTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Node node;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, TravelingSalesman2.class);
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode(Label.label("User"));
            node.setProperty("name", "foo");
            tx.commit();
        }
    }

    // todo - mettere in pathFindingTest
    private static final String SETUP_GEO = "CREATE (b:City {name:'Berlin', coords: point({latitude:52.52464,longitude:13.40514}), lat:52.52464,lon:13.40514})\n" +
            "CREATE (m:City {name:'München', coords: point({latitude:48.1374,longitude:11.5755}), lat:48.1374,lon:11.5755})\n" +
            "CREATE (f:City {name:'Frankfurt',coords: point({latitude:50.1167,longitude:8.68333}), lat:50.1167,lon:8.68333})\n" +
            "CREATE (h:City {name:'Hamburg', coords: point({latitude:53.554423,longitude:9.994583}), lat:53.554423,lon:9.994583})\n" +
            "CREATE (b)-[:DIRECT {dist:255.64*1000}]->(h)\n" +
            "CREATE (b)-[:DIRECT {dist:504.47*1000}]->(m)\n" +
            "CREATE (b)-[:DIRECT {dist:424.12*1000}]->(f)\n" +
            "CREATE (f)-[:DIRECT {dist:304.28*1000}]->(m)\n" +
            "CREATE (f)-[:DIRECT {dist:393.15*1000}]->(h)";


    @Test
    public void testTravelingSalesman() throws Exception {
        db.executeTransactionally(SETUP_GEO);
        // path -> {VirtualPath@12018} "(3)-[TEST,-1]->(4)-[TEST,-2]->(1)-[TEST,-3]->(2)"
        TestUtil.testResult(db, "MATCH (n:City) with collect(n) as nodes " +
                "call apoc.algo.traveling(nodes, {}) yield path return path", Map.of(), r -> {
            final List<Map<String, Object>> maps = Iterators.asList(r);
            System.out.println("TravelingSalesmanTest.testTravelingSalesman");
            
        });
//        final double v = new TravelingSalesman2.SimulatedAnnealing().simulateAnnealing(10, 1000, 0.9);
        System.out.println("TravelingSalesmanTest.testTravelingSalesman");
    }


//    @Test
//    public void testTravelingSalesman() throws Exception {
//        final City[] cities = List.of(/*new City("zero", 1.00D, 1.01D),
//                new City("uno", 1D, 1.1D),*/
//                new City("due", 1D, 1.2D),
//                new City("tre", 1D, 1.3D),
//                new City("quattro", 1D, 1.4D)
//        ).toArray(new City[0]);
//
//        final TravellingSalesmanSolver travellingSalesmanSolver = new TravellingSalesmanSolver(cities, 5D);
//        final City[] cityList = travellingSalesmanSolver.getCityList();
//
//        final TravellingSalesmanSolver travellingSalesmanSolver2 = new TravellingSalesmanSolver(cities, 1D);
//        final City[] cityList2 = travellingSalesmanSolver.getCityList();
//        System.out.println("TravelingSalesmanTest.testTravelingSalesman");
//
////        List list = asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
////        assertEquals(1, Util.partitionSubList(list,0).count());
////        assertEquals(1,Util.partitionSubList(list,1).count());
////        assertEquals(2,Util.partitionSubList(list,2).count());
////        assertEquals(3,Util.partitionSubList(list,3).count());
////        assertEquals(4,Util.partitionSubList(list,4).count());
////        assertEquals(5,Util.partitionSubList(list,5).count());
////        assertEquals(5,Util.partitionSubList(list,6).count());
////        assertEquals(5,Util.partitionSubList(list,7).count());
////        assertEquals(5,Util.partitionSubList(list,8).count());
////        assertEquals(5,Util.partitionSubList(list,9).count());
////        assertEquals(10,Util.partitionSubList(list,10).count());
////        assertEquals(10,Util.partitionSubList(list,11).count());
////        assertEquals(10,Util.partitionSubList(list,20).count());
//    }
}
