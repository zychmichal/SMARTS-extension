package processor.worker;

import common.Settings;
import traffic.TrafficNetwork;
import traffic.road.Edge;
import traffic.road.RoadNetwork;
import traffic.routing.Dijkstra;
import traffic.routing.RandomAStar;
import traffic.routing.RouteLeg;
import traffic.routing.Routing;
import traffic.vehicle.DriverProfile;
import traffic.vehicle.Vehicle;
import traffic.vehicle.VehicleType;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class CreateVehiclesService implements Runnable {

    private Semaphore semaphore = new Semaphore(1);
    private TrafficNetwork trafficNetwork;
    Routing routingAlgorithm;
    Random random = new Random();
    final int oneStepCapacity = (int) ((Settings.numGlobalRandomBackgroundPrivateVehicles * WorkerManager.percentToCreate) / (Settings.numWorkers * 100.0));
    HashMap <String, List<Vehicle>> vehiclesInAllWorkers = new HashMap<>();
    ArrayList<Worker> workers = new ArrayList<>();
    WorkerManager workerManager;
    List<String> accessWorkersInIteration;

    public CreateVehiclesService(WorkerManager workerManager, RoadNetwork roadNetwork){
        this.workerManager = workerManager;
        this.trafficNetwork = new TrafficNetwork(roadNetwork);
        if (Settings.routingAlgorithm == Routing.Algorithm.DIJKSTRA) {
            this.routingAlgorithm = new Dijkstra(this.trafficNetwork);
        } else if (Settings.routingAlgorithm == Routing.Algorithm.RANDOM_A_STAR) {
            this.routingAlgorithm = new RandomAStar(this.trafficNetwork);
        }
    }

    public synchronized void addWorker(Worker worker, ArrayList<Vehicle> reserveVehicles){
        workers.add(worker);
        vehiclesInAllWorkers.put(worker.name, reserveVehicles);           //here should be additional array list with vehicles
    }

    @Override
    public void run() {
        if(semaphore.tryAcquire()){
            accessWorkersInIteration = workers.stream()
                    .map(w -> w.name)
                    .filter(name -> vehiclesInAllWorkers.get(name).size() < oneStepCapacity)
                    .collect(Collectors.toList());
            while(!(accessWorkersInIteration.isEmpty())){
                createVehicle();
            }
            semaphore.release();
        }
    }

    public void createVehicle(){
        Worker worker = getRandomPossibleWorker();
        if(worker != null){
            var vehicleList = vehiclesInAllWorkers.get(worker.name);
            var possibleStartEdges = worker.trafficNetwork.internalNonPublicVehicleStartEdges;
            var possibleEndEdges = worker.trafficNetwork.internalNonPublicVehicleEndEdges;
            var vehicleType = chooseType();
            var routes = createOneRandomInternalRoute(vehicleType, possibleStartEdges, possibleEndEdges);
            if(routes == null) return;
            var randomProfile = worker.trafficNetwork.getRandomDriverProfile(this.random);
            var internalPrefix = worker.trafficNetwork.internalVehiclePrefix;
            Vehicle vehicle = worker.trafficNetwork.createVehicle(vehicleType, false, false, routes, internalPrefix, -1.0, "", randomProfile);

            if(WorkerManager.printStream) System.out.printf("CREATE VEHICLE FOR WORKER: %s\n", worker.name);
            synchronized (vehicleList) {
                vehicleList.add(vehicle);
            }
            if(vehicleList.size() >= oneStepCapacity){// tu dla jednego wiec bez synchronized sie obejdzie
                accessWorkersInIteration.remove(worker.name);
            }
        }
    }

    private Worker getRandomPossibleWorker() {
        String name = "";
        try {
            name = accessWorkersInIteration.get(new Random().nextInt(accessWorkersInIteration.size()));
        } catch (IllegalArgumentException | IndexOutOfBoundsException e){
            return null;
        }
        String finalName = name;
        return workers.stream().filter(w -> finalName.equals(w.name)).findAny().orElse(null);
    }

    ArrayList<RouteLeg> createOneRandomInternalRoute(final VehicleType type, ArrayList<Edge> internalNonPublicVehicleEndEdges, ArrayList<Edge> internalNonPublicVehicleStartEdges ) {
        final Edge edgeStart = internalNonPublicVehicleStartEdges
                .get(random.nextInt(internalNonPublicVehicleStartEdges.size()));

        final Edge edgeEnd = internalNonPublicVehicleEndEdges
                .get(random.nextInt(internalNonPublicVehicleEndEdges.size()));

        final ArrayList<RouteLeg> route = routingAlgorithm.createCompleteRoute(edgeStart, edgeEnd, type);

        if ((route == null) || (route.size() == 0) || !(internalNonPublicVehicleStartEdges.contains(route.get(0).edge))) {
            return null;
        } else {
            return route;
        }
    }

    public VehicleType chooseType(){
        final double typeDecider = random.nextDouble();
        VehicleType type;
        if (typeDecider < 0.05) {
            type = VehicleType.BIKE;
        } else if ((0.05 <= typeDecider) && (typeDecider < 0.1)) {
            type = VehicleType.TRUCK;
        } else {
            type = VehicleType.CAR;
        }
        return type;
    }
}
