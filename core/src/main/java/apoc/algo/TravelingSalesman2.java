package apoc.algo;

import apoc.result.VirtualNode;
import apoc.result.VirtualPath;
import apoc.result.VirtualRelationship;
//import org.apache.commons.math3.optim.linear.Relationship;
import apoc.util.Util;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
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

    public static class TravelingSalesmanConfig {

        private final Double coolingFactor;
        private final Double startingTemperature;
        private final Double endTemperature;

        public TravelingSalesmanConfig(Map<String, Object> config) {
            if (config == null) config = Collections.emptyMap();
            this.coolingFactor = Util.toDouble(config.getOrDefault("coolingFactor", "MD5"));
            this.startingTemperature = Util.toDouble(config.getOrDefault("startingTemperature", "MD5"));
            this.endTemperature = Util.toDouble(config.getOrDefault("endTemperature", "MD5"));
        }

        public Double getCoolingFactor() {
            return coolingFactor;
        }

        public Double getStartingTemperature() {
            return startingTemperature;
        }

        public Double getEndTemperature() {
            return endTemperature;
        }
    }


    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure("apoc.algo.traveling")
    @Description("apoc.algo.traveling(nodes,  ...) - todo")
    public Stream<DistancePathResult> travelSalesman(
            @Name("startNode") List<Node> nodes,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        TravelingSalesmanConfig conf = new TravelingSalesmanConfig(config);
        // todo - configs in un oggetto..
        final Double coolingFactor = conf.getCoolingFactor();
        if (coolingFactor < 1) {
            throw new RuntimeException("todo");
        }
        final Double endTemperature = conf.getEndTemperature();
        final Double startingTemperature = conf.getStartingTemperature();
        if (coolingFactor < 0 || endTemperature < 0 || startingTemperature < 0) {
            throw new RuntimeException("todo");
        }
        return Stream.of(new SimulatedAnnealing().simulateAnnealing(nodes, "lat", "lon", startingTemperature, endTemperature, coolingFactor));

//        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
//                new BasicEvaluationContext(tx, db),
//                buildPathExpander(relTypesAndDirs),
//                CommonEvaluators.doubleCostEvaluator(weightPropertyName),
//                CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName));
//        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }
    
    public static class SimulatedAnnealing {

        private List<Node> currentTravel = new ArrayList<>();
        private List<Node> newTravel = new ArrayList<>();
        
        private EstimateEvaluator<Double> evaluator;

        public void swapCities() {
            int a = generateRandomIndex();
            int b = generateRandomIndex();
            while(a == b) {
                b = generateRandomIndex();
            }
            newTravel = new ArrayList<>(currentTravel);
            Collections.swap(newTravel, a, b);
        }

        public void revertSwap() {
            currentTravel = newTravel;
        }

        private int generateRandomIndex() {
            return ThreadLocalRandom.current().nextInt(0, currentTravel.size());
        }

        public double getDistance(List<Node> travels) {
            return IntStream.rangeClosed(1, travels.size())
                    .mapToDouble(idx -> {
                        Node starting = travels.get(idx - 1);
                        Node destination = travels.get(idx == travels.size() ? 0 : idx);
                        return evaluator.getCost(starting, destination);
                    }).sum();
        }

//        private static Travel travel = new Travel(10);

        public DistancePathResult simulateAnnealing(List<Node> nodes, String latPropertyName, String lonPropertyName, double startingTemperature, double endingTemperature, double coolingRate) {
            currentTravel = nodes;
            evaluator = CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName);

            double temp = startingTemperature;
//            travel.generateInitialTravel();
            double bestDistance = getDistance(currentTravel);
//            double bestDistance = travel.getDistance();
            System.out.println("Initial distance of travel: " + bestDistance);
//            List<Node> bestSolution = travel; // todo - nel path mettere una relazione virtuale customizzabile...
//            List<Node> currentSolution = bestSolution;

            while (temp > endingTemperature) {
                swapCities();


                // Get energy of solutions
                double currentDistance = getDistance(currentTravel);
                double newDistance = getDistance(newTravel);


                
                
                // Decide if we should accept the neighbour
                double ap = Math.exp((currentDistance - newDistance) / temp);
                if (ap > Math.random()) {
//                if (Utility.acceptanceProbability(currentDistance, newDistance, temp) > rand) {
//                    revertSwap();
//                    currentTravel = newTravel;
                    System.out.println("ap = " + ap);
                    revertSwap();
//                    currentSolution = new Tour(newSolution.getTour());
                }
                
                // Keep track of the best solution found
                if (getDistance(currentTravel) < bestDistance) {
//                if (currentSolution.getTotalDistance() < best.getTotalDistance()) {
//                    best = new Tour(currentSolution.getTour());
                    System.out.println("ap1 = " + ap);
                    bestDistance = currentDistance;
                }

                // decrement temp via coolingRate
                temp *= coolingRate;
            }

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

}
