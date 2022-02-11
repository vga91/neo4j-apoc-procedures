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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class TravelingSalesman2 {
    public static class DistancePathResult {
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
        private final String latitudeProp;
        private final String longitudeProp;
        private final String relName;

        public TravelingSalesmanConfig(Map<String, Object> config) {
            if (config == null) config = Collections.emptyMap();
            this.coolingFactor = Util.toDouble(config.getOrDefault("coolingFactor", 0.995));
            this.startingTemperature = Util.toDouble(config.getOrDefault("startingTemperature", 100000));
            this.endTemperature = Util.toDouble(config.getOrDefault("endTemperature", 0.1));
            this.latitudeProp = (String) config.getOrDefault("latitudeProp", "latitude");
            this.longitudeProp = (String) config.getOrDefault("longitudeProp", "longitude");
            this.relName = (String) config.getOrDefault("relName", "CONNECT_TO");
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

        public String getLatitudeProp() {
            return latitudeProp;
        }

        public String getLongitudeProp() {
            return longitudeProp;
        }

        public String getRelName() {
            return relName;
        }
    }


    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure("apoc.algo.traveling")
    @Description("apoc.algo.traveling(nodes,  $config) - traveling salesman via simulated annealing")
    public Stream<DistancePathResult> travelSalesman(
            @Name("startNode") List<Node> nodes,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        TravelingSalesmanConfig conf = new TravelingSalesmanConfig(config);
        return Stream.of(new SimulatedAnnealing(nodes).simulateAnnealing(conf));
    }
    
    public static class SimulatedAnnealing {

        private List<Node> currentTravel;

        private List<Node> newTravel = new ArrayList<>();
        private EstimateEvaluator<Double> evaluator;

        public SimulatedAnnealing(List<Node> currentTravel) {
            this.currentTravel = currentTravel;
        }

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

        public DistancePathResult simulateAnnealing(TravelingSalesmanConfig config) {
            final double coolingFactor = config.getCoolingFactor();
            if (coolingFactor > 1) {
                throw new RuntimeException("coolingFactor must be less than 1");
            }
            final double endTemperature = config.getEndTemperature();
            final double startingTemperature = config.getStartingTemperature();
            if (coolingFactor < 0 || endTemperature < 0 || startingTemperature < 0) {
                throw new RuntimeException("coolingFactor, endTemperature amd startingTemperature must be positive");
            }
            
            evaluator = CommonEvaluators.geoEstimateEvaluator(config.getLatitudeProp(), config.getLongitudeProp());

            double temperature = config.getStartingTemperature();
            double bestDistance = getDistance(currentTravel);
            
            while (temperature > config.getEndTemperature()) {
                swapCities();
                
                // Get energy of solutions
                double currentDistance = getDistance(currentTravel);
                double newDistance = getDistance(newTravel);
                
                // Decide if we should accept the neighbour
                if (Math.exp((currentDistance - newDistance) / temperature) > Math.random()) {
                    revertSwap();
//                    currentSolution = new Tour(newSolution.getTour());
                }
                
                // Keep track of the best solution found
                if (getDistance(currentTravel) < bestDistance) {
                    bestDistance = currentDistance;
                }

                // decrement temp via coolingFactor
                temperature *= config.getCoolingFactor();
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
                        final VirtualRelationship vRel = new VirtualRelationship(start, end,  withName(config.getRelName()));
                        virtualPath.addRel(vRel);
                    });
            return new DistancePathResult(virtualPath, bestDistance);
        }

    }

}
