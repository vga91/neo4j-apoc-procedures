package apoc.algo;

import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.Utils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
//import static apoc.algo.TravellingSalesmanSolver.City;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
// -- E010IT0db4ec02f479ceb3760b64b5785c8998f <https://www-m9.ma.tum.de/games/tsp-game/index_en.html
    // todo - mettere in pathFindingTest
    private static final String SETUP_GEO = "CREATE (:City {name:'Brescia', lat:45.541553,lon:10.211802})\n" +
            "CREATE (:City {name:'Genova', lat:37.95, lon:12.7})\n" +
        "CREATE (:City {name:'Milano', lat:45.4654219, lon:9.1859243})\n" +
        "CREATE (:City {name:'Firenze', lat:43.833333,lon:11.333333})\n" +
            "CREATE (:City {name:'Frosinone', lat:41.633333,lon:13.316667})\n" +
            "CREATE (:City {name:'Messina', lat:38.1938137,lon:15.5540152})\n" +
            "CREATE (:City {name:'Catanzaro', lat:38.9,lon:16.583333})\n" +
            "CREATE (:City {name:'Cosenza', lat:39.3,lon:16.25})\n" +
            "CREATE (:City {name:'Salerno', lat:40.6824408,lon:14.7680961})\n" +
            "CREATE (:City {name:'Lecce',  lat:40.35481,lon:18.172073})";


    @Test
    public void testTravelingSalesman() throws Exception {
        db.executeTransactionally(SETUP_GEO);
        // path -> {VirtualPath@12018} "(3)-[TEST,-1]->(4)-[TEST,-2]->(1)-[TEST,-3]->(2)"
        TestUtil.testCall(db, "MATCH (n:City) with collect(n) as nodes " +
                "call apoc.algo.traveling(nodes, {}) yield path, distance return path, distance", Map.of(), r -> {
            Path path = (Path) r.get("path");
            double distance = (double) r.get("distance");
            final List<Object> name = Iterables.stream(path.nodes())
                    .map(i -> i.getProperty("name"))
                    .collect(Collectors.toList());
            assertTrue(distance < 3000000);
            
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
