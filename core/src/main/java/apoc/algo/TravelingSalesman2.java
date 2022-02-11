package apoc.algo;

import apoc.result.VirtualNode;
import apoc.result.VirtualPath;
import apoc.result.VirtualRelationship;
//import org.apache.commons.math3.optim.linear.Relationship;
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
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class TravelingSalesman2 {
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
                new SimulatedAnnealing().simulateAnnealing(nodes, "lat", "lon", 100000, 1000000, 0.995));

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

        private List<Node> currentTravel = new ArrayList<>();
        private List<Node> newTravel = new ArrayList<>();
        
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

//        public void swapCities() {
//            int a = generateRandomIndex();
//            int b = generateRandomIndex();
//            while(tourPos1 == tourPos2) {tourPos2 = Utility.randomInt(0 , newSolution.tourSize());}
//            previousTravel = new ArrayList<>(travels);
////            previousTravel = new ArrayList<>(travels);
//            Node x = travels.get(a);
//            Node y = travels.get(b);
//            travels.set(a, y);
//            travels.set(b, x);
//        }

        public void swapCities() {
            int a = generateRandomIndex();
            int b = generateRandomIndex();
            while(a == b) {b = Utility.randomInt(0 , newTravel.size());}
            newTravel = new ArrayList<>(currentTravel);
//            previousTravel = new ArrayList<>(travels);
            // todo - collection - swap?
            Node x = newTravel.get(a);
            Node y = newTravel.get(b);
            newTravel.set(a, y);
            newTravel.set(b, x);
        }

        public void revertSwap() {
            currentTravel = newTravel;
        }

        private int generateRandomIndex() {
            return Utility.randomInt(0 , currentTravel.size());
        }

        public Node getCity(int index) {
            return currentTravel.get(index);
        }

        public double getDistance(List<Node> travels) {
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
            currentTravel = nodes;
            evaluator = CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName);

            double temp = startingTemperature;
//            travel.generateInitialTravel();
            double bestDistance = getDistance(currentTravel);
//            double bestDistance = travel.getDistance();
            System.out.println("Initial distance of travel: " + bestDistance);
//            List<Node> bestSolution = travel; // todo - nel path mettere una relazione virtuale customizzabile...
//            List<Node> currentSolution = bestSolution;

//            while (temp > 0.1) {
//            for (double t = temperature; t > 1; t *= coolingFactor) {
            for (int i = 0; i < numberOfIterations; i++) {
//            for (int i = 0; i < numberOfIterations; i++) {
//            while (temp > 0.05) { // todo - questo temp potrei customizzarlo...
                // Create new neighbour tour
//                Tour newSolution = new Tour(currentSolution.getTour());

                swapCities();
//                // Get random positions in the tour
//                int tourPos1 = Utility.randomInt(0 , newSolution.tourSize());
//                int tourPos2 = Utility.randomInt(0 , newSolution.tourSize());
//
//                //to make sure that tourPos1 and tourPos2 are different
//                while(tourPos1 == tourPos2) {tourPos2 = Utility.randomInt(0 , newSolution.tourSize());}
//
//                // Get the cities at selected positions in the tour
//                City citySwap1 = newSolution.getCity(tourPos1);
//                City citySwap2 = newSolution.getCity(tourPos2);
//
//                // Swap them
//                newSolution.setCity(tourPos2, citySwap1);
//                newSolution.setCity(tourPos1, citySwap2);

                // Get energy of solutions
                double currentDistance   = getDistance(currentTravel);
                double newDistance = getDistance(newTravel);


                
                
                // Decide if we should accept the neighbour
                double ap = Math.exp((currentDistance - newDistance) / temp);
                if (ap > Math.random()) {
//                if (Utility.acceptanceProbability(currentDistance, newDistance, temp) > rand) {
//                    revertSwap();
                    currentTravel = newTravel;
//                    currentSolution = new Tour(newSolution.getTour());
                }
                
                // Keep track of the best solution found
                if (getDistance(currentTravel) < bestDistance) {
//                if (currentSolution.getTotalDistance() < best.getTotalDistance()) {
//                    best = new Tour(currentSolution.getTour());
                    bestDistance = currentDistance;
                }

                // Cool system
                temp = temp * coolingRate;
//                temp *= 1 - coolingRate;
            }
//                if (t > 0.1) {
////                    currentSolution.swapCities();
//                    swapCities();
//                    double currentDistance = getDistance();
////                    double currentDistance = currentSolution.getDistance();
//                    if (currentDistance < bestDistance) {
//                        bestDistance = currentDistance;
//                    } else if (Math.exp((bestDistance - currentDistance) / t) < Math.random()) {
//                        revertSwap();
////                        currentSolution.revertSwap();
//                    }
//                    t *= coolingRate;
//                } else {
//                    // todo - toglierlo , non si capisce
//                    // oppure mttere errore, mettere numero maggiore throw new RuntimeExceptuon()
//                    continue;
//                }
//                if (i % 100 == 0) {
//                    System.out.println("Iteration #" + i);
//                }
//            }

            final int size = currentTravel.size();
            
            
            final VirtualNode node = VirtualNode.from(currentTravel.get(0));
            final VirtualPath virtualPath = new VirtualPath(node);
            if (size == 1) {
                return new DistancePathResult(new VirtualPath(node), bestDistance);
            }
            IntStream.range(0, size - 1)
                    .forEach(idx -> {
                        final VirtualNode start = idx == 0 ? node : VirtualNode.from(currentTravel.get(idx));
                        VirtualNode end = VirtualNode.from(currentTravel.get(idx + 1));
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

    public static class Utility {


//        /**
//         * Computes and returns the Euclidean distance between two cities
//         * @param city1 the first city
//         * @param city2 the second city
//         * @return distance the dist between city1 and city2
//         */
//        public static double distance(City city1, City city2){
//            int xDistance = Math.abs(city1.getX() - city2.getX());
//            int yDistance = Math.abs(city1.getY() - city2.getY());
//            double distance = Math.sqrt( (xDistance*xDistance) + (yDistance*yDistance) );
//
//            return distance;
//        }

        /**
         * Calculates the acceptance probability
         * @param currentDistance the total distance of the current tour
         * @param newDistance the total distance of the new tour
         * @param temperature the current temperature
         * @return value the probability of whether to accept the new tour
         */
        public static double acceptanceProbability(double currentDistance, double newDistance, double temperature) {
            // If the new solution is better, accept it
            if (newDistance > currentDistance) {
                return 0.0;
            }
            // If the new solution is worse, calculate an acceptance probability
            return Math.exp((currentDistance - newDistance) / temperature);
        }

        /**
         * this method returns a random number n such that
         * 0.0 <= n <= 1.0
         * @return random such that 0.0 <= random <= 1.0
         */
        static double randomDouble()
        {
            Random r = new Random();
            return r.nextInt(1000) / 1000.0;
        }

        /**
         * returns a random int value within a given range
         * min inclusive .. max not inclusive
         * @param min the minimum value of the required range (int)
         * @param max the maximum value of the required range (int)
         * @return rand a random int value between min and max [min,max)
         */
        public static int randomInt(int min , int max) {
            Random r = new Random();
            double d = min + r.nextDouble() * (max - min);
            return (int)d;
        }
    }

}
