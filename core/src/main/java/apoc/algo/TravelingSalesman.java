package apoc.algo;

import apoc.result.VirtualNode;
import apoc.result.VirtualPath;
import apoc.result.VirtualRelationship;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class TravelingSalesman {
    
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure("apoc.algo.travelSalesman")
    @Description("apoc.algo.travelSalesman(nodes,  $config) - resolve traveling salesman problem via simulated annealing algo")
    public Stream<DistancePathResult> travelSalesman(@Name("startNode") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (nodes.isEmpty()) {
            throw new RuntimeException("The nodes parameter must have at least 3 nodes");
        }
        TravelingSalesmanConfig conf = new TravelingSalesmanConfig(config);
        return Stream.of(SimulatedAnnealing.simulateAnnealing(nodes, conf));
    }

    public static class Tour {
        private final List<Node> travel;
        private final EstimateEvaluator<Double> evaluator;

        public Tour(List<Node> travel, EstimateEvaluator<Double> evaluator) {
            this.travel = new ArrayList<>(travel);
            this.evaluator = evaluator;
        }

        public void swapCities() {
            int a = generateRandomIndex();
            int b = generateRandomIndex();
            while(a == b) {
                b = generateRandomIndex();
            }
            Collections.swap(travel, a, b);
        }
        
        private int generateRandomIndex() {
            return (int) (travel.size() * Math.random());
        }

        public double getDistance() {
            return IntStream.rangeClosed(1, travel.size())
                    .mapToDouble(idx -> {
                        Node starting = travel.get(idx - 1);
                        Node destination = travel.get(idx == travel.size() ? 0 : idx);
                        return evaluator.getCost(starting, destination);
                    }).sum();
        }
        
        public Tour copy() {
            return new Tour(travel, evaluator);
        }
    }
    
    public static class SimulatedAnnealing {

        public static DistancePathResult simulateAnnealing(List<Node> cities, TravelingSalesmanConfig config) {
            final double coolingFactor = config.getCoolingFactor();
            if (coolingFactor > 1) {
                throw new RuntimeException("coolingFactor must be less than 1");
            }
            final double endTemperature = config.getEndTemperature();
            final double startTemperature = config.getStartTemperature();
            if (coolingFactor < 0 || endTemperature < 0 || startTemperature < 0) {
                throw new RuntimeException("coolingFactor, endTemperature amd startTemperature must be positive");
            }

            EstimateEvaluator<Double> evaluator = CommonEvaluators.geoEstimateEvaluator(config.getLatitudeProp(), config.getLongitudeProp());

            Tour current = new Tour(cities, evaluator);
            Tour best = current.copy();
            
            double temperature = config.getStartTemperature();
            while (temperature > config.getEndTemperature()) {
                Tour neighbor = current.copy();
                neighbor.swapCities();
                
                // Get distance of current and new (swapped) travel
                double currentDistance = current.getDistance();
                double newDistance = neighbor.getDistance();
                
                // Decide if we should accept the new result
                if (Math.random() < Math.exp((currentDistance - newDistance) / temperature)) {
                    current = neighbor.copy();
                }
                
                // Keep the best distance found
                if (current.getDistance() < best.getDistance()) {
                    best = current.copy();
                }

                // decrement temp via coolingFactor
                temperature *= config.getCoolingFactor();
            }
            
            // return virtual path result
            final List<VirtualNode> vNodes = best.travel.stream()
                    .map(VirtualNode::from)
                    .collect(Collectors.toList());

            final VirtualNode node = vNodes.get(0);
            final VirtualPath virtualPath = new VirtualPath(node);
            
            IntStream.range(0, best.travel.size() - 1)
                    .forEach(i -> {
                        final VirtualRelationship vRel = new VirtualRelationship(
                                vNodes.get(i), vNodes.get(i + 1), withName(config.getRelName()));
                        virtualPath.addRel(vRel);
                    });
            
            return new DistancePathResult(virtualPath, best.getDistance());
        }
    }

    public static class DistancePathResult {
        public Path path;
        public double distance;

        public DistancePathResult(Path path, double distance) {
            this.path = path;
            this.distance = distance;
        }
    }

    private static class TravelingSalesmanConfig {

        private final Double coolingFactor;
        private final Double startTemperature;
        private final Double endTemperature;
        private final String latitudeProp;
        private final String longitudeProp;
        private final String relName;

        public TravelingSalesmanConfig(Map<String, Object> config) {
            if (config == null) config = Collections.emptyMap();
            this.coolingFactor = Util.toDouble(config.getOrDefault("coolingFactor", 0.995));
            this.startTemperature = Util.toDouble(config.getOrDefault("startTemperature", 100000));
            this.endTemperature = Util.toDouble(config.getOrDefault("endTemperature", 0.1));
            this.latitudeProp = (String) config.getOrDefault("latitudeProp", "latitude");
            this.longitudeProp = (String) config.getOrDefault("longitudeProp", "longitude");
            this.relName = (String) config.getOrDefault("relName", "CONNECT_TO");
        }

        public Double getCoolingFactor() {
            return coolingFactor;
        }

        public Double getStartTemperature() {
            return startTemperature;
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
}
