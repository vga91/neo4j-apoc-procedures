package apoc.algo;

import apoc.result.VirtualNode;
import apoc.result.VirtualPath;
import apoc.result.VirtualRelationship;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class TravelingSalesman2CheDovrebbeAndareBene {
    public static class DistancePathResult { // TODO: derive from PathResult when access to derived properties is fixed for yield
        public Path path;
        public double distance;

        public DistancePathResult(Path path, double distance) {
            this.path = path;
            this.distance = distance;
        }
    }



    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure("apoc.algo.traveling")
    @Description("apoc.algo.traveling(nodes,  ...) - todo")
    public Stream<DistancePathResult> aStar(
            @Name("startNode") List<Node> nodes,
//            @Name("endNode") Node endNode,
//            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
//            @Name("weightPropertyName") String weightPropertyName,
//            @Name("latPropertyName") String latPropertyName,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        
        // todo - configs in un oggetto..
        return Stream.of(//new PathResult(
                new SimulatedAnnealing().simulateAnnealing(nodes, "lat", "lon", 1000, 1000, 0.9));

//        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
//                new BasicEvaluationContext(tx, db),
//                buildPathExpander(relTypesAndDirs),
//                CommonEvaluators.doubleCostEvaluator(weightPropertyName),
//                CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName));
//        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }
    
//    public static class Travel {
//
// 
//
//    }


//    public static class Point {
//
//        private int x;
//        private int y;
//
//        public Point() {
//            this.x = (int) (Math.random() * 500);
//            this.y = (int) (Math.random() * 500);
//        }
//
//        // todo - shortest path??
//        public double distanceToCity(Point city) {
//            int x = Math.abs(getX() - city.getX());
//            int y = Math.abs(getY() - city.getY());
//            return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
//        }
//
//        public int getX() {
//            return x;
//        }
//
//        public int getY() {
//            return y;
//        }
//    }
    
    public static class SimulatedAnnealing {

        private List<Node> travels = new ArrayList<>();
        private List<Node> previousTravel = new ArrayList<>();
        
        private EstimateEvaluator<Double> evaluator;
//        public Travel(ArrayList<Point> points) {
//            travel = points;
////            for (int i = 0; i < numberOfCities; i++) {
////                travel.add(new Point());
////            }
//        }

//        public void generateInitialTravel() {
//            if (travel.isEmpty()) {
//                new Travel(10);
//            }
//            Collections.shuffle(travel);
//        }

        public void swapCities() {
            int a = generateRandomIndex();
            int b = generateRandomIndex();
            previousTravel = new ArrayList<>(travels);
//            previousTravel = new ArrayList<>(travels);
            Node x = travels.get(a);
            Node y = travels.get(b);
            travels.set(a, y);
            travels.set(b, x);
        }

        public void revertSwap() {
            travels = previousTravel;
        }

        private int generateRandomIndex() {
            return (int) (Math.random() * travels.size());
        }

        public Node getCity(int index) {
            return travels.get(index);
        }

        public double getDistance() {
            double distance = 0D;
            // todo - rifare il for
            for (int index = 0; index < travels.size(); index++) {
                Node starting = getCity(index);
                Node destination;
                if (index + 1 < travels.size()) {
                    destination = getCity(index + 1);
                } else {
                    destination = getCity(0);
                }
//                distance += starting.distanceToCity(destination);
                distance += evaluator.getCost(starting, destination);// starting.distanceToCity(destination);
            }
            return distance;
        }

//        private static Travel travel = new Travel(10);

        public DistancePathResult simulateAnnealing(List<Node> nodes, String latPropertyName, String lonPropertyName, double startingTemperature, int numberOfIterations, double coolingRate) {
            travels = nodes;
            evaluator = CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName);

            double t = startingTemperature;
//            travel.generateInitialTravel();
            double bestDistance = getDistance();
//            double bestDistance = travel.getDistance();
            System.out.println("Initial distance of travel: " + bestDistance);
//            List<Node> bestSolution = travel; // todo - nel path mettere una relazione virtuale customizzabile...
//            List<Node> currentSolution = bestSolution;

            for (int i = 0; i < numberOfIterations; i++) {
                if (t > 0.1) {
//                    currentSolution.swapCities();
                    swapCities();
                    double currentDistance = getDistance();
//                    double currentDistance = currentSolution.getDistance();
                    if (currentDistance < bestDistance) {
                        bestDistance = currentDistance;
                    } else if (Math.exp((bestDistance - currentDistance) / t) < Math.random()) {
                        revertSwap();
//                        currentSolution.revertSwap();
                    }
                    t *= coolingRate;
                } else {
                    // todo - toglierlo , non si capisce
                    // oppure mttere errore, mettere numero maggiore throw new RuntimeExceptuon()
                    continue;
                }
                if (i % 100 == 0) {
                    System.out.println("Iteration #" + i);
                }
            }

            final int size = travels.size();
            
            
            final VirtualNode node = VirtualNode.from(travels.get(0));
            final VirtualPath virtualPath = new VirtualPath(node);
            if (size == 1) {
                return new DistancePathResult(new VirtualPath(node), bestDistance);
            }
            IntStream.range(0, size - 1)
                    .forEach(idx -> {
                        final VirtualNode start = idx == 0 ? node : VirtualNode.from(travels.get(idx));
                        VirtualNode end = VirtualNode.from(travels.get(idx + 1));
                        final VirtualRelationship vRel = new VirtualRelationship(start, end,  withName("TEST"));
                        virtualPath.addRel(vRel);
                    });

            // 2728553.0653759595
            // 2604145.385010691
            System.out.println(bestDistance);
            return new DistancePathResult(virtualPath, bestDistance);
//            travels.forEach(i -> {
//                if (i > 0) {
//                    
//                }
//            });
//            return travels;
//            return bestDistance;
        }

    }

}
