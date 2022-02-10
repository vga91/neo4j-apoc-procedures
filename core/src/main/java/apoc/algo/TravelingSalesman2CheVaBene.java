package apoc.algo;

import java.util.ArrayList;
import java.util.Collections;

public class TravelingSalesman2CheVaBene {
    public static class Travel {

        private ArrayList<City> travel = new ArrayList<>();
        private ArrayList<City> previousTravel = new ArrayList<>();

        public Travel(int numberOfCities) {
            for (int i = 0; i < numberOfCities; i++) {
                travel.add(new City());
            }
        }

        public void generateInitialTravel() {
            if (travel.isEmpty()) {
                new Travel(10);
            }
            Collections.shuffle(travel);
        }

        public void swapCities() {
            int a = generateRandomIndex();
            int b = generateRandomIndex();
            previousTravel = new ArrayList<>(travel);
            City x = travel.get(a);
            City y = travel.get(b);
            travel.set(a, y);
            travel.set(b, x);
        }

        public void revertSwap() {
            travel = previousTravel;
        }

        private int generateRandomIndex() {
            return (int) (Math.random() * travel.size());
        }

        public City getCity(int index) {
            return travel.get(index);
        }

        public int getDistance() {
            int distance = 0;
            for (int index = 0; index < travel.size(); index++) {
                City starting = getCity(index);
                City destination;
                if (index + 1 < travel.size()) {
                    destination = getCity(index + 1);
                } else {
                    destination = getCity(0);
                }
                distance += starting.distanceToCity(destination);
            }
            return distance;
        }

    }


    public static class City {

        private int x;
        private int y;

        public City() {
            this.x = (int) (Math.random() * 500);
            this.y = (int) (Math.random() * 500);
        }

        // todo - shortest path??
        public double distanceToCity(City city) {
            int x = Math.abs(getX() - city.getX());
            int y = Math.abs(getY() - city.getY());
            return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }
    }
    
    public static class SimulatedAnnealing {

        private static Travel travel = new Travel(10);

        public static double simulateAnnealing(double startingTemperature, int numberOfIterations, double coolingRate) {
            System.out.println("Starting SA with temperature: " + startingTemperature + ", # of iterations: " + numberOfIterations + " and colling rate: " + coolingRate);
            double t = startingTemperature;
            travel.generateInitialTravel();
            double bestDistance = travel.getDistance();
            System.out.println("Initial distance of travel: " + bestDistance);
            Travel bestSolution = travel;
            Travel currentSolution = bestSolution;

            for (int i = 0; i < numberOfIterations; i++) {
                if (t > 0.1) {
                    currentSolution.swapCities();
                    double currentDistance = currentSolution.getDistance();
                    if (currentDistance < bestDistance) {
                        bestDistance = currentDistance;
                    } else if (Math.exp((bestDistance - currentDistance) / t) < Math.random()) {
                        currentSolution.revertSwap();
                    }
                    t *= coolingRate;
                } else {
                    continue;
                }
                if (i % 100 == 0) {
                    System.out.println("Iteration #" + i);
                }
            }
            return bestDistance;
        }

    }

}
