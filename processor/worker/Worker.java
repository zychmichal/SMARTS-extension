package processor.worker;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import common.Settings;
import common.SysUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import osm.OSM;
import processor.communication.IncomingConnectionBuilder;
import processor.communication.MessageHandler;
import processor.communication.MessageProcessor;
import processor.communication.MessageSender;
import processor.communication.message.*;
import processor.server.DataOutputScope;
import processor.util.WorkerTimings;
import traffic.TrafficNetwork;
import traffic.light.LightUtil;
import traffic.road.*;
import traffic.routing.RouteLeg;
import traffic.routing.RouteUtil;
import traffic.vehicle.DriverProfile;
import traffic.vehicle.TrajectoryUtil;
import traffic.vehicle.Vehicle;
import traffic.vehicle.VehicleType;

import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;

/**
 * Worker receives simulation configuration from server and simulates traffic in
 * a specific area. Operations include:
 * <ul>
 * <li>Initialize work area
 * <li>Generate vehicles
 * <li>Update status of vehicles and traffic lights
 * <li>Exchange traffic information with neighbor workers
 * <li>Report result back to server
 * </ul>
 *
 * Note: IP/port of worker should be directly accessible as the information will
 * be used by server to build TCP connection with the worker.
 *
 * Simulation runs in server-based mode (BSP) or server-less mode (PSP). In BSP,
 * a worker needs to wait two messages from server at each time step: one asking
 * the worker to share traffic information with its fellow workers, another
 * asking the worker to simulate (computing models for vehicles and traffic
 * lights). In PSP, worker synchronize simulation with its fellow workers
 * automatically. Worker can send information about vehicles and traffic lights
 * to server in both modes, if server asks the worker to do so during setup. The
 * information can be used to update GUI at server or a remote controller.
 */
public class Worker implements MessageHandler, Runnable {
	class ConnectionBuilderTerminationTask extends TimerTask {
		@Override
		public void run() {
			connectionBuilder.terminate();
		}
	}

	/**
	 * Starts worker and tries to connect with server
	 *
	 * @param args server address (optional)
	 */
	public static void main(final String[] args) {
		if (args.length > 0) {
			Settings.serverAddress = args[0];
		}
        try{
            new Worker().run();
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1000);
        }

	}
	//ROAD NETWORK FROM MANAGER
    RoadNetwork roadNetwork = null;

    private int createVehicle = 0;
	WorkerManager workerManager;
	Worker me = this;
	IncomingConnectionBuilder connectionBuilder;
	public TrafficNetwork trafficNetwork;
	int step = 0;
	double timeNow;
	ArrayList<Fellow> fellowWorkers = new ArrayList<>();
	public String name = "";
	MessageSender senderForServer;
	MessageSender senderForMe;
	public String address = "localhost";
	public int listeningPort;
    private boolean responsibleForService = false;
	Workarea workarea;
	ArrayList<Fellow> connectedFellows = new ArrayList<>();// Fellow workers that share at least one edge with this
	// worker
	ArrayList<Message_WW_Traffic> receivedTrafficCache = new ArrayList<>();
	Simulation simulation;
	boolean isDuringServerlessSim;// Once server-less simulation begins, this will true until the simulation ends
	boolean isPausingServerlessSim;
	boolean isSimulatingOneStep;
	//To simulate all lanes and edges in workarea without whole map
	HashMap<Integer, Lane> lanesInWorkarea = new HashMap<>();
    HashMap<Integer, Edge> pspBorderEdgesMap = new HashMap<>();
    HashMap<Integer, Edge> pspNonBorderEdgesMap = new HashMap<>();
	ArrayList<Edge> pspBorderEdges = new ArrayList<>();// For PSP (server-less)
	ArrayList<Edge> pspNonBorderEdges = new ArrayList<>();// For PSP (server-less)


	Thread singleWorkerServerlessThread = new Thread();// Used when this worker is the only worker in server-less mode
	int numVehicleCreatedSinceLastSetupProgressReport = 0;
	int numLocalRandomPrivateVehicles = 0;
	int numLocalRandomTrams = 0;
	int numLocalRandomBuses = 0;

	WorkerTimings workerTimings = new WorkerTimings();
	MessageProcessor messageProcessor;
	FileOutputWorker fileOutputW;
	private static Logger logger;
	Runtime runtime = Runtime.getRuntime();
	java.lang.management.ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    //Number of finish vehicles per step
    int finishVehicles = 0;

	public Worker(){}

    public Worker(RoadNetwork roadNetwork, WorkerManager wm){
	    this.roadNetwork = roadNetwork;
	    workerManager = wm;
    }

    public Worker(RoadNetwork roadNetwork, WorkerManager wm, boolean responsibleForService){
        this.roadNetwork = roadNetwork;
        workerManager = wm;
        this.responsibleForService = responsibleForService;
    }

	void changeLaneBlock(int laneIndex, boolean isBlocked) {
	    //workarea enough instead of roadNetwork
        //shuld change to copy of workarea
		trafficNetwork.roadNetwork.lanes.get(laneIndex).isBlocked = isBlocked;
	}

	void buildThreadForSingleWorkerServerlessSimulation() {
		singleWorkerServerlessThread = new Thread() {
			public void run() {

				while (step < Settings.maxNumSteps) {

					if (!isDuringServerlessSim) {
						break;
					}

					while (isPausingServerlessSim) {
						try {
							this.sleep(1);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							logger.error("InterruptedException --- buildThreadForSingleWorkerServerlessSimulation(): ", e);
						}
					}

					timeNow = step / Settings.numStepsPerSecond;
					simulateOneStep(me, true, true, true);

					sendTrafficReportInServerlessMode();

					step++;
				}
				// Finish simulation
                workerManager.incrementFinishSimulation(new Message_WS_Serverless_Complete(name, step, trafficNetwork.vehicles.size()));

			}
		};
	}

	void sendTrafficReportInServerlessMode() {
		logger.debug("start sending [WS_TrafficReport] step: {}", step);
		workerTimings.startSendingWSreport();
		if ((step + 1) % Settings.trafficReportStepGapInServerlessMode == 0) {
            workerManager.saveTrafficReportIndex(new Message_WS_TrafficReport(name, trafficNetwork.vehicles, trafficNetwork.lightCoordinator,
                    trafficNetwork.newVehiclesSinceLastReport, step, trafficNetwork.numInternalNonPublicVehicle,
                    trafficNetwork.numInternalTram, trafficNetwork.numInternalBus));
			trafficNetwork.clearReportedData();
		}
		workerTimings.stopSendingWSreport();
		logger.debug("end sending [WS_TrafficReport] step: {}", step);
	}

	Vehicle createReceivedVehicle(final SerializableVehicle serializableVehicle) {
	    //here check routes -> if in psp/non psp  => change route
		final Vehicle vehicle = new Vehicle();
		vehicle.type = VehicleType.getVehicleTypeFromName(serializableVehicle.type);
		vehicle.length = vehicle.type.length;
		vehicle.routeLegs = RouteUtil.parseReceivedRoute(serializableVehicle.routeLegs, trafficNetwork.roadNetwork.edges);
		vehicle.trajectories = TrajectoryUtil.parseReceivedTrajectories(serializableVehicle.trajectories);
		vehicle.indexLegOnRoute = serializableVehicle.indexRouteLeg;
		vehicle.lane = trafficNetwork.roadNetwork.lanes.get(serializableVehicle.laneIndex);
		vehicle.headPosition = serializableVehicle.headPosition;
		vehicle.speed = serializableVehicle.speed;
		vehicle.timeRouteStart = serializableVehicle.timeRouteStart;
		vehicle.id = serializableVehicle.id;
		vehicle.isExternal = serializableVehicle.isExternal;
		vehicle.isForeground = serializableVehicle.isForeground;
		vehicle.idLightGroupPassed = serializableVehicle.idLightGroupPassed;
		vehicle.driverProfile = DriverProfile.valueOf(serializableVehicle.driverProfile);
		vehicle.workerName = this.name;
		changeVehicleLaneInWorker(vehicle);
		changeVehicleRoutesEdgesInWorker(vehicle);
//        System.out.println("TRANSFER VEHICLE");
		return vehicle;
	}

	void changeVehicleLaneInWorker(Vehicle vehicle){
	    Lane lane = lanesInWorkarea.get(vehicle.lane.index);
	    if(lane != null){
            vehicle.lane = lane;
        }
    }
	//To change route in copy edges not in whole roadNetwork but only in Workarea
	public void changeVehicleRoutesEdgesInWorker(Vehicle vehicle){

	    for(int i=0; i < vehicle.routeLegs.size(); i++){
	        int actualEdgeIndex = vehicle.routeLegs.get(i).edge.index;
	        if(pspNonBorderEdgesMap.containsKey(actualEdgeIndex)){
                vehicle.routeLegs.get(i).edge = pspNonBorderEdgesMap.get(actualEdgeIndex);
            }
            else if(pspBorderEdgesMap.containsKey(actualEdgeIndex)){
                vehicle.routeLegs.get(i).edge = pspBorderEdgesMap.get(actualEdgeIndex);
            }
        }
    }

	void createVehiclesFromFellow(final Message_WW_Traffic messageToProcess) {
		for (final SerializableVehicle serializableVehicle : messageToProcess.vehiclesEnteringReceiver) {
			final Vehicle vehicle = createReceivedVehicle(serializableVehicle);
			trafficNetwork.addOneTransferredVehicle(vehicle, timeNow);
		}
	}

	/**
	 * Divide edges overlapping with the responsible area of this worker into two
	 * sets based on their closeness to the border of the responsible area.
	 */
	void divideLaneSetForServerlessSim() {
		final HashSet<Edge> edgeBorderSet = new HashSet<>();
		final HashSet<Edge> edgeBorderFellowsSet = new HashSet<>();
        final HashSet<Edge> edgeNonBorderSet = new HashSet<>();

		for (final Fellow fellow : connectedFellows) {
			for (final Edge e : fellow.inwardEdgesAcrossBorder) {
				edgeBorderSet.add(e);
				edgeBorderSet.addAll(findInwardEdgesWithinCertainDistance(e.startNode, 0, 28.0 / Settings.numStepsPerSecond,
						edgeBorderSet));
				double proper_distance = Math.max(Settings.lookAheadDistance, 28.0 / Settings.numStepsPerSecond);
                edgeBorderFellowsSet.addAll(findOutwardEdgesWithinCertainDistance(e.endNode, e.length, proper_distance,
                        edgeBorderFellowsSet));
			}
		}
        for (final Edge e: edgeBorderSet) {
            Edge newEdge = e.copyThisEdge();
            pspBorderEdgesMap.put(e.index, newEdge);
            for(Lane l: newEdge.lanes) lanesInWorkarea.put(l.index, l);
        }

		pspBorderEdges = new ArrayList<>(pspBorderEdgesMap.values());


		for (final GridCell gridCell : workarea.getWorkCells()){
            for (final Node node: gridCell.nodes){
                for (final Edge e: node.inwardEdges){
                    if (!pspBorderEdgesMap.containsKey(e.index)){
                        edgeNonBorderSet.add(e);
                    }
                }
                for (final Edge e: node.outwardEdges){
                    if (!pspBorderEdgesMap.containsKey(e.index)){
                        edgeNonBorderSet.add(e);
                    }
                }
            }
        }

        for (final Edge e: edgeNonBorderSet) {
            Edge newEdge = e.copyThisEdge();
            pspNonBorderEdgesMap.put(e.index, newEdge);
            for(Lane l: newEdge.lanes) lanesInWorkarea.put(l.index, l);
        }

//        for (final Edge e : edgeBorderFellowsSet) {
//            for (Fellow fellow: connectedFellows){
//                if(fellow.workarea.getWorkCells().contains(e.startNode.gridCell) && fellow.workarea.getWorkCells().contains(e.endNode.gridCell)){
//                    counterFellow++;
//                }
//            }
//        }

        if(WorkerManager.printStream) System.out.printf("%s: edgeNonBorderSet size: %d, edgeBorderSet size: %d\n", name, edgeNonBorderSet.size(), edgeBorderSet.size());


        pspNonBorderEdges = new ArrayList<>(pspNonBorderEdgesMap.values());
		/*for (final Edge e : trafficNetwork.edges) {
			if (!edgeSet.contains(e)) {
				pspNonBorderEdges.add(e);
			}
		}*/


//		System.out.println(trafficNetwork.roadNetwork.edges.size());
//        System.out.println(pspNonBorderEdges.size());
//        System.out.println(pspBorderEdges.size());
//        System.out.println(counter);
//        System.out.println(fellowWorkers.stream().map(x -> x.outwardEdgesAcrossBorder.size()).mapToInt(Integer::valueOf).sum());

	}

	/**
	 * Find the fellow workers that need to communicate with this worker. Each of
	 * these fellow workers shares at least one edge with this worker.
	 */
	void findConnectedFellows() {
		connectedFellows.clear();
		for (final Fellow fellowWorker : fellowWorkers) {
			if ((fellowWorker.inwardEdgesAcrossBorder.size() > 0)
					|| (fellowWorker.outwardEdgesAcrossBorder.size() > 0)) {
				connectedFellows.add(fellowWorker);
			}
		}
	}

	HashSet<Edge> findInwardEdgesWithinCertainDistance(final Node node, final double accumulatedDistance,
													   final double maxDistance, final HashSet<Edge> edgesToSkip) {
		for (final Edge e : node.inwardEdges) {
			if (edgesToSkip.contains(e)) {
				continue;
			} else {
				edgesToSkip.add(e);
				final double updatedAccumulatedDistance = accumulatedDistance + e.length;
				if (updatedAccumulatedDistance < maxDistance) {
					findInwardEdgesWithinCertainDistance(e.startNode, accumulatedDistance, maxDistance, edgesToSkip);
				}
			}
		}
		return edgesToSkip;
	}

    HashSet<Edge> findOutwardEdgesWithinCertainDistance(final Node node, final double accumulatedDistance,
                                                       final double maxDistance, final HashSet<Edge> edgesToSkip) {
        for (final Edge e : node.outwardEdges) {
            if (edgesToSkip.contains(e) || workarea.workCells.contains(e.endNode.gridCell)) {
                continue;
            } else {
                edgesToSkip.add(e);
                final double updatedAccumulatedDistance = accumulatedDistance + e.length;
                if (updatedAccumulatedDistance < maxDistance) {
                    findOutwardEdgesWithinCertainDistance(e.endNode, accumulatedDistance, maxDistance, edgesToSkip);
                }
            }
        }
        return edgesToSkip;
    }

	boolean isAllFellowsAtState(final FellowState state) {
		int count = 0;
		for (final Fellow w : connectedFellows) {
			if (w.state == state) {
				count++;
			}
		}

		if (count == connectedFellows.size()) {
			return true;
		} else {
			return false;
		}

	}

	/**
	 * Create a new worker. Set the worker's name, address and listening port. The
	 * worker comes with a new receiver for receiving messages, e.g., connection
	 * requests, from other entities such as workers.
	 */
	void join() {
		// Get IP address
        String ipAddress = SysUtil.getMyIpV4Addres();

        join(ipAddress);
	}

	void join(String address){
	    this.address = address;

        // Find an available port
        listeningPort = Settings.serverListeningPortForWorkers + 1
                + (new Random()).nextInt(65535 - Settings.serverListeningPortForWorkers);
        ServerSocket ss;
        while (true) {
            try {
                ss = new ServerSocket(listeningPort);
                break;
            } catch (final IOException e) {
                listeningPort = 50000 + (new Random()).nextInt(65535 - 50000);
            }
        }

        name = address + ':' + listeningPort;
        if(WorkerManager.printStream) System.out.println("Worker(" + name + ") STARTED!" );

        // name for log file
        System.setProperty("logFilename", name.replace(":","-"));
        System.setProperty("logNodeIP", name.split(":")[0]);

        logger = LogManager.getLogger("WorkerLogger");

        fileOutputW = new FileOutputWorker();
        fileOutputW.init(name);

        // Prepare message processing units
        //HERE CHANGE
        messageProcessor = new MessageProcessor(name, 24, this);
        messageProcessor.runAllProccessors();

        connectionBuilder = new IncomingConnectionBuilder(messageProcessor, listeningPort, this, ss);
        connectionBuilder.setName("[" + name + "][ConnectionBuilder]");
        connectionBuilder.start();

        senderForMe = new MessageSender(this.address, this.listeningPort, name);
//        senderForServer = new MessageSender(Settings.serverAddress, Settings.serverListeningPortForWorkers, name);

        workarea = new Workarea(name, null);

//        senderForServer.send(new Message_WS_Join(name, address, listeningPort));

        if(WorkerManager.printStream) System.out.println("Worker (" + name + ") FINISH CREATING!");
        workerManager.incrementDoneCreateWorker(this);
        //ITERATE TO WM -> TO SEND MESSAGE TO WORKER THAT INITIALIZATION IS COMPLETED
    }



	void proceedBasedOnSyncMethod() {
		// In case neighbor already sent traffic for this step
		processCachedReceivedTraffic();

		proceedBasedOnSync();

	}

    void proceedBasedOnSync() {
        if (isAllFellowsAtState(FellowState.SHARED)) {
            if (Settings.isServerBased) {
                senderForServer.send(new Message_WS_ServerBased_SharedMyTrafficWithNeighbor(name));
            } else if (isDuringServerlessSim) {
                sendTrafficReportInServerlessMode();

                workerTimings.processOneStepTimers();
                if (step % WorkerTimings.NUMBER_OF_SAMPLES == 0 && WorkerManager.printStream) {
                    if (step != 0) {
                        workerTimings.printResults(System.out, name, step, trafficNetwork.numInternalNonPublicVehicle);
                    }
                    workerTimings.resetAll();
                }

                // Proceed to next step or finish
                if (step >= Settings.maxNumSteps) {
                    if(WorkerManager.printStream) System.out.printf("MAX NUM FINISH FOR WORKER: %s\n", name);
                    workerManager.incrementFinishSimulation(new Message_WS_Serverless_Complete(name, step, trafficNetwork.vehicles.size()));

                    logger.info("[Message_WS_Serverless_Complete] Sent to server.");
                    if (Settings.collectTrajectoryInWorkers) {
                        dumpTrajectoriesToFiles();
                    }
                    workerManager.incrementFinishTrajectory();
//                    senderForServer.send(new Message_WS_DumpingDone());
                    logger.info("[Message_WS_DumpingDone] Sent to server.");
                    if(!Settings.oneSimulation){
                        resetTraffic();
                    }

                } else if (!isPausingServerlessSim) {
                    step++;
                    timeNow = step / Settings.numStepsPerSecond;
                    if (step%250==0) {
                        logger.info("Simulating step({})", step);
                        //logMachineState();
                    }

                    simulateOneStep(this, true, true, true);
                    logger.debug("End of simulation step({})", step);


                    proceedBasedOnSyncMethod();
                }
            }
        }
        workerTimings.resetIdle();
    }


	void processCachedReceivedTraffic() {
		workerTimings.resetProcessReceivedWW();  // tu mozna sumarycznie na krok czasowy ile tego jest
		final Iterator<Message_WW_Traffic> iMessage = receivedTrafficCache.iterator();

		while (iMessage.hasNext()) {
			final Message_WW_Traffic message = iMessage.next();
			if (message.stepAtSender == step) {
				logger.debug("Start processing cached msg for step: {}", step);
				processReceivedTraffic(message);
				iMessage.remove();
			}
		}
		workerTimings.updateProcessReceivedWW();
	}

	ArrayList<GridCell> processReceivedGridCells(final ArrayList<SerializableGridCell> received,
												 final GridCell[][] grid) {
		final ArrayList<GridCell> cellsInWorkarea = new ArrayList<>();
		for (final SerializableGridCell receivedCell : received) {
			cellsInWorkarea.add(grid[receivedCell.row][receivedCell.column]);
		}
		return cellsInWorkarea;
	}

	void processReceivedMetadataOfWorkers(final ArrayList<SerializableWorkerMetadata> metadataWorkers) {
		// Set work area of all workers
		for (final SerializableWorkerMetadata metadata : metadataWorkers) {
			final ArrayList<GridCell> cellsInWorkarea = processReceivedGridCells(metadata.gridCells,
					trafficNetwork.roadNetwork.grid);
			if (metadata.name.equals(name)) {
				workarea.setWorkCells(cellsInWorkarea);
			} else {
				final Fellow fellow = new Fellow(metadata.name, metadata.address, metadata.port, cellsInWorkarea);
				fellowWorkers.add(fellow);
			}
		}

		// Identify edges shared with fellow workers
		for (final Fellow fellowWorker : fellowWorkers) {
			fellowWorker.getEdgesFromAnotherArea(workarea);
			fellowWorker.getEdgesToAnotherArea(workarea);
		}

		// Identify fellow workers that share edges with this worker
		findConnectedFellows();

		// Prepare communication with the fellow workers that share edges with
		// this worker
		for (final Fellow fellowWorker : connectedFellows) {
			fellowWorker.prepareCommunication();
		}
	}

	@Override
	public synchronized void processReceivedMsg(final Object message) {
		if (message instanceof Message_SW_Setup) {
			workerTimings.startProcesingWholeSetupTimer();
			final Message_SW_Setup messageToProcess = (Message_SW_Setup) message;
			logger.info("[Msg_SW_Setup] - maxNumSteps: "+ Settings.maxNumSteps+", Q: " + messageProcessor.getQueueSize());

			workerTimings.startProcesReceivedSimConfTimer();
			processReceivedSimulationConfiguration(messageToProcess);
			workerTimings.stopProcesReceivedSimConfTimer();

            if(WorkerManager.printStream) System.out.printf("START BUILD ENV, name: %s\n", name);
			workerTimings.startBuildEnvTimer();
			trafficNetwork.buildEnvironment(workarea.workCells, workarea.workerName, step);
			workerTimings.stopBuildEnvTimer();

            if(WorkerManager.printStream) System.out.printf("START RESET TRAFFIC, name: %s\n", name);
			workerTimings.startResetTrafficTimer();
			resetTraffic();
			workerTimings.stopResetTrafficTimer();

			isSimulatingOneStep = false;

			// Pause a bit so other workers can reset before starting simulation
			try {
				Thread.sleep(500);
			} catch (final InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("InterruptedException --- while sleeping in processReceivedMsg(Message_SW_Setup): ", e);
			}

            if(WorkerManager.printStream) System.out.printf("START INIT LIGHT COORDINATOR, name: %s\n", name);
			if ((messageToProcess.indexNodesToAddLight.size() > 0)
					|| (messageToProcess.indexNodesToRemoveLight.size() > 0)) {
				trafficNetwork.lightCoordinator.init(trafficNetwork.roadNetwork.nodes, messageToProcess.indexNodesToAddLight,
						messageToProcess.indexNodesToRemoveLight, workarea);
			}

			// Reset fellow state
			for (final Fellow connectedFellow : connectedFellows) {
				connectedFellow.state = FellowState.SHARED;
			}

            if(WorkerManager.printStream) System.out.printf("START CREATE VEHICLES, name: %s\n", name);
            if(WorkerManager.readRoutes){
                workerManager.incrementStartCreatingVehiclesFromFiles();
            }else{
                // Create vehicles
                numVehicleCreatedSinceLastSetupProgressReport = 0;
//                final TimerTask progressTimerTask = new TimerTask() {
//                    @Override
//                    public void run() {
////                    System.out.printf("SEND Message_WS_SetupCreatingVehicles, name: %s\n", name);
//                        senderForServer.send(new Message_WS_SetupCreatingVehicles(
//                                trafficNetwork.vehicles.size() - numVehicleCreatedSinceLastSetupProgressReport));
//                        numVehicleCreatedSinceLastSetupProgressReport = trafficNetwork.vehicles.size();
//                    }
//                };
//
//                final Timer progressTimer = new Timer();
//                final Random random = new Random();

//                if (Settings.isVisualize) {
//                    progressTimer.scheduleAtFixedRate(progressTimerTask, 500, random.nextInt(1000) + 1);
//                }

                if(WorkerManager.printStream){
                    System.out.println("MEMORY BEFORE CREATING VEHICLES, " + name);
                    workerManager.logAndPrintMachineState(0);
                }

                workerTimings.startExternalVehicleTimer();
                trafficNetwork.createExternalVehicles(messageToProcess.externalRoutes, timeNow);
                workerTimings.stopExternalVehicleTimer();

                workerTimings.startInternalVehicleTimer();
                System.out.printf("START CREATE INTERNAL VEHICLES, name: %s, vehicles to create: %d\n", name, numLocalRandomPrivateVehicles);
                trafficNetwork.createInternalVehicles(numLocalRandomPrivateVehicles, numLocalRandomTrams,
                        numLocalRandomBuses, true, true, true, timeNow);
                workerTimings.stopInternalVehicleTimer();

                if(WorkerManager.useServiceToCreateVehicles){
                    ArrayList<Vehicle> reserveVehicles = new ArrayList<>();
                    int vehiclesToCreate = workerManager.createVehiclesService.oneStepCapacity;
                    System.out.printf("START CREATE RESERVE VEHICLES, name: %s, vehicles to create: %d\n", name, vehiclesToCreate);
                    trafficNetwork.createInternalReservedVehiclesToService(vehiclesToCreate, reserveVehicles);
                    workerManager.createVehiclesService.addWorker(this, reserveVehicles);
                }
                System.out.printf("STOP CREATE VEHICLES, name: %s\n", name);

//                progressTimerTask.cancel();
//                progressTimer.cancel();

                finishSetup();
            }

		} else if (message instanceof Message_SW_ServerBased_ShareTraffic) {
			final Message_SW_ServerBased_ShareTraffic messageToProcess = (Message_SW_ServerBased_ShareTraffic) message;

			step = messageToProcess.currentStep;
			timeNow = step / Settings.numStepsPerSecond;
			transferVehicleDataToFellow();
			proceedBasedOnSyncMethod();
		} else if (message instanceof Message_WW_Traffic) {
			final Message_WW_Traffic messageToProcess = (Message_WW_Traffic) message;
			logger.debug("[Msg_WW_Traffic] from: " + messageToProcess.senderName + ", s: "+ messageToProcess.stepAtSender + ", Q: " + messageProcessor.getQueueSize());
//			System.out.println("[Msg_WW_Traffic] to: " + name + ", s: "+ messageToProcess.stepAtSender + ", Q: " + messageProcessor.getQueueSize());

			workerTimings.updateIdle();
			receivedTrafficCache.add(messageToProcess);
			proceedBasedOnSyncMethod();
		} else if (message instanceof Message_SW_ServerBased_Simulate) {
			final Message_SW_ServerBased_Simulate messageToProcess = (Message_SW_ServerBased_Simulate) message;

			simulateOneStep(this, messageToProcess.isNewNonPubVehiclesAllowed,
					messageToProcess.isNewTramsAllowed, messageToProcess.isNewBusesAllowed);
			senderForServer
					.send(new Message_WS_TrafficReport(name, trafficNetwork.vehicles, trafficNetwork.lightCoordinator,
							trafficNetwork.newVehiclesSinceLastReport, step, trafficNetwork.numInternalNonPublicVehicle,
							trafficNetwork.numInternalTram, trafficNetwork.numInternalBus));
			trafficNetwork.clearReportedData();
		} else if (message instanceof Message_SW_Serverless_Start) {
			final Message_SW_Serverless_Start messageToProcess = (Message_SW_Serverless_Start) message;
			step = messageToProcess.startStep;
			isDuringServerlessSim = true;
			isPausingServerlessSim = false;

			workerTimings.startWholeStep();

            timeNow = step / Settings.numStepsPerSecond;
            simulateOneStep(this, true, true, true);
            proceedBasedOnSyncMethod();

			/*if (connectedFellows.size() == 0) {
				logger.warn("Building single Thread simulation - seems that only one worker is connected.");
				buildThreadForSingleWorkerServerlessSimulation();
				singleWorkerServerlessThread.start();
			} else {
				timeNow = step / Settings.numStepsPerSecond;
				simulateOneStep(this, true, true, true);
			}*/
		} else if (message instanceof Message_SW_KillWorker) {
			logger.info("[Msg_SW_KillWorker] - Queue: " + messageProcessor.getQueueSize());

			final Message_SW_KillWorker messageToProcess = (Message_SW_KillWorker) message;
			//kill message processing units
			messageProcessor.poisonAllProcessors();

			// Quit depending on how the worker was started
			if (messageToProcess.isSharedJVM) {
				final ConnectionBuilderTerminationTask task = new ConnectionBuilderTerminationTask();
                new Timer().schedule(task, 1000);  // does not work bufferreader should be closed in different way
			} else {
				System.exit(0);
			}
		} else if (message instanceof Message_SW_Serverless_Stop) {
			logger.info("[Msg_SW_Serverless_Stop] - Queue: " + messageProcessor.getQueueSize());

			isDuringServerlessSim = false;
			isPausingServerlessSim = false;
			singleWorkerServerlessThread.stop();
			resetTraffic();
		} else if (message instanceof Message_SW_Serverless_Pause) {
			isPausingServerlessSim = true;
		} else if (message instanceof Message_SW_Serverless_Resume) {
			isPausingServerlessSim = false;
			// When it is not single worker environment, explicitly resume the routine tasks
			if (connectedFellows.size() > 0) {
				proceedBasedOnSyncMethod();
			}
		} else if (message instanceof Message_SW_ChangeSpeed) {
			final Message_SW_ChangeSpeed messageToProcess = (Message_SW_ChangeSpeed) message;
			Settings.pauseTimeBetweenStepsInMilliseconds = messageToProcess.pauseTimeBetweenStepsInMilliseconds;
		} else if (message instanceof Message_SW_BlockLane) {
			final Message_SW_BlockLane messageToProcess = (Message_SW_BlockLane) message;
			changeLaneBlock(messageToProcess.laneIndex, messageToProcess.isBlocked);
		} else if (message instanceof Message_WW_Same) {

            workerTimings.updateIdle();
            proceedBasedOnSyncMethod();
        } else {
            System.out.println("NOT MATCHING MESSAGE");
        }
		logger.debug("Releasing lock...");
	}

    public void createVehicleFromRoutesLines(ArrayList<String []> lines){
        System.out.println("WORKER + " + name + " CREATE FROM FILE START");
        System.out.println("WORKER " + name + " NR OF VEHICLES: " + lines.size());
        var start = System.nanoTime();
        for(var line: lines){
            if(createVehicle >= numLocalRandomPrivateVehicles){
                break;
            }
            VehicleType type = VehicleType.getVehicleTypeFromName(line[0]);
            ArrayList<RouteLeg> route = new ArrayList<>();
            for(int i = 1; i < line.length; i++){
                Edge e = trafficNetwork.roadNetwork.edges.get(Integer.parseInt(line[i]));
                route.add(new RouteLeg(e, 0));
            }
            createInitialVehicle(type, route);
            createVehicle++;
        }
        System.out.println("WORKER + " + name + " CREATE FROM FILE STOP, time: " + (System.nanoTime() - start/1e9));
    }

    public void createVehiclesFromBatch(ArrayList<String []> lines){
        createVehicleFromRoutesLines(lines);

        workerManager.incrementToCreateFromFile(this, createVehicle >= numLocalRandomPrivateVehicles);

    }

    public void finishCreatingVehicles(ArrayList<String []> lines){
        System.out.println(numLocalRandomPrivateVehicles);
        createVehicleFromRoutesLines(lines);
        int vehiclesToCreate = numLocalRandomPrivateVehicles - createVehicle;
        System.out.println("WORKER " + name + " VEHICLES TO CREATE " + vehiclesToCreate);
        if(vehiclesToCreate > 0){
            trafficNetwork.createInternalNonPublicVehicles(vehiclesToCreate,timeNow);
        }

        finishSetup();
    }

    public void createInitialVehicle(VehicleType type, ArrayList<RouteLeg> route){
        trafficNetwork.addNewVehicle(type, false, false, route, trafficNetwork.internalVehiclePrefix, timeNow, "",
                trafficNetwork.getRandomDriverProfile());
    }



    public void finishSetup() {
        StringBuilder fellowList = new StringBuilder("[");
        for(Fellow fellow : connectedFellows){
            fellowList.append(fellow.name);
            fellowList.append(',');
        }
        fellowList.append(']');

        logger.info("[SETUP_DONE] - "
                +" Q: " + messageProcessor.getQueueSize()
                + ", #fellows: "+ connectedFellows.size()
                + ", f_list: " + fellowList.toString());
        workerTimings.stopProcesingWholeSetupTimer();

        // Let server know that setup is done
        workerTimings.startSendingSetupDoneTimer();
//            System.out.printf("SEND SETUP DONE, name: %s\n", name);
//        senderForServer.send(new Message_WS_SetupDone(name, connectedFellows.size()));
        workerManager.incrementDoneSetupWorker();

        workerTimings.stopSendingSetupDoneTimer();

        if(WorkerManager.printStream) workerTimings.printProcessingSetupElapsedTime(name, System.out);
        workerTimings.logAllSetupTimers(logger);
    }

    void processReceivedSimulationConfiguration(final Message_SW_Setup received) {
		logger.debug("SETUP_FROM_WORKER" + received.toString());
		numLocalRandomPrivateVehicles = received.numRandomPrivateVehicles;
		numLocalRandomTrams = received.numRandomTrams;
		numLocalRandomBuses = received.numRandomBuses;


		if (received.isNewEnvironment) {
			trafficNetwork = new TrafficNetwork(name, this.roadNetwork, this);
			processReceivedMetadataOfWorkers(received.metadataWorkers);

			simulation = new Simulation(trafficNetwork, connectedFellows);
			divideLaneSetForServerlessSim();
		}

		// Reset lights
		trafficNetwork.lightCoordinator.init(trafficNetwork.roadNetwork.nodes, new ArrayList<SerializableInt>(),
				new ArrayList<SerializableInt>(), workarea);
	}

	/*private boolean buildOSMmap(final Message_SW_Setup received) {
		final OSM osm = new OSM();
		Settings.inputOpenStreetMapFile = received.inputOpenStreetMapFile;
		osm.processOSM(Settings.inputOpenStreetMapFile, true, "WORKER_" + name);
		Settings.isBuiltinRoadGraph = false;
		// Revert to built-in map if there was an error when converting new map
		if (Settings.roadGraph.length() == 0) {
			try{
				throw new Exception("MAP NOT PARSED CORRECTLY by changeMap() method");
			}catch (Exception e){
				System.err.println("[SERVER][ERROR] MAP WAS NOT PARSED CORRECTLY BY SERVER!!!");
				e.printStackTrace();
				logger.error("Exception in buildOSMmap(): ", e);
			}
			Settings.roadGraph = RoadUtil.importBuiltinRoadGraphFile();
			Settings.isBuiltinRoadGraph = true;
		}
//				osm.dumpMapToFileDebugMethod("Worker_" + name.replace(':','-') + "_Roads.txt");
		return true;
	}*/

	void processReceivedTraffic(final Message_WW_Traffic messageToProcess) {
		createVehiclesFromFellow(messageToProcess);
		updateTrafficAtOutgoingEdgesToFellows(messageToProcess);
        updateFellowState(messageToProcess.senderName, FellowState.SHARING_DATA_RECEIVED);
	}

	void resetTraffic() {

		for (final Fellow fellow : fellowWorkers) {
			fellow.vehiclesToCreateAtBorder.clear();
			fellow.state = FellowState.SHARED;
		}

		receivedTrafficCache.clear();
		for (final Edge edge : pspBorderEdges) {
			for (final Lane lane : edge.lanes) {
				lane.vehicles.clear();
			}
		}
		for (final Edge edge : pspNonBorderEdges) {
			for (final Lane lane : edge.lanes) {
				lane.vehicles.clear();
			}
		}

		trafficNetwork.resetTraffic();
	}

	void dumpTrajectoriesToFiles(){
		connectionBuilder.preTerminateChannels();

		logger.info("Started dumping trajectories.");
		workerTimings.startDumpingTrajecTimer();
		fileOutputW.outputTrajectories(trafficNetwork.vehicles);
		fileOutputW.close();
		workerTimings.stopDumpingTrajecTimer();
		logger.info("Finished dumping trajectories into file.");

		workerTimings.logDumpingTrajectories(logger);
		workerTimings.printDumpTrajFromWorkerTime(name, System.out);
	}

	@Override
	public void run() {
		// Join system by connecting with server
		join();
	}

	/**
	 * Set the percentage of drivers with different profiles, from highly aggressive
	 * to highly polite.
	 */
	ArrayList<Double> setDriverProfileDistribution(final ArrayList<SerializableDouble> sList) {
		final ArrayList<Double> list = new ArrayList<>();
		for (final SerializableDouble sd : sList) {
			list.add(sd.value);
		}
		return list;
	}

	ArrayList<double[]> setRouteSourceDestinationWindow(final ArrayList<Serializable_GPS_Rectangle> sList) {
		final ArrayList<double[]> list = new ArrayList<>();
		for (final Serializable_GPS_Rectangle sgr : sList) {
			list.add(new double[] { sgr.minLon, sgr.maxLat, sgr.maxLon, sgr.minLat });
		}
		return list;
	}

	/**
	 * Send vehicle position on cross-border edges to fellow workers.
	 */
	void transferVehicleDataToFellow() {
		workerTimings.startSendingWWmsgs();
		for (final Fellow fellowWorker : connectedFellows) {
			fellowWorker.send(new Message_WW_Traffic(name, fellowWorker, step));
			updateFellowState(fellowWorker.name, FellowState.SHARING_DATA_SENT);
			fellowWorker.vehiclesToCreateAtBorder.clear();
		}
		workerTimings.stopSendingWWmsgs();
	}

	void unblockLanes(final ArrayList<SerializableLaneIndex> unblockedLaneIndex) {
		for (final SerializableLaneIndex item : unblockedLaneIndex) {
			trafficNetwork.roadNetwork.lanes.get(item.index).isBlocked = false;
		}
	}

	void updateFellowState(final String workerName, final FellowState newState) {
		for (final Fellow fellow : connectedFellows) {
			if (fellow.name.equals(workerName)) {
				if ((fellow.state == FellowState.SHARING_DATA_RECEIVED)
						&& (newState == FellowState.SHARING_DATA_SENT)) {
					fellow.state = FellowState.SHARED;
				} else if ((fellow.state == FellowState.SHARING_DATA_SENT)
						&& (newState == FellowState.SHARING_DATA_RECEIVED)) {
					fellow.state = FellowState.SHARED;
				} else {
					fellow.state = newState;
				}
				break;
			}
		}
	}

	void updateTrafficAtOutgoingEdgesToFellows(final Message_WW_Traffic received) {
		for (final SerializableFrontVehicleOnBorder info : received.lastVehiclesLeftReceiver) {
			final int laneIndex = info.laneIndex;
			final double position = info.endPosition;
			final double speed = info.speed;
			trafficNetwork.roadNetwork.lanes.get(laneIndex).endPositionOfLatestVehicleLeftThisWorker = position;
			trafficNetwork.roadNetwork.lanes.get(laneIndex).speedOfLatestVehicleLeftThisWorker = speed;
		}
	}

	private synchronized void simulateOneStep(Worker worker, boolean isNewNonPubVehiclesAllowed,
											  boolean isNewTramsAllowed, boolean isNewBusesAllowed){
//        System.out.println("WORKER " + name + " simulate step: " + step + " with: " + trafficNetwork.vehicles.size() + " vehicles");
        workerTimings.startSimulationOfOneStep();
        //HERE RUN SERVICE IF IT IS NON ACTIVE!
        if(responsibleForService){
            workerManager.poolService.execute(workerManager.createVehiclesService);
        }
	    simulation.simulateOneStep(worker, isNewNonPubVehiclesAllowed, isNewTramsAllowed, isNewBusesAllowed);
		workerTimings.stopSimulationOfOneStep();

        if (WorkerManager.useServiceToCreateVehicles) getVehiclesFromServiceCreateAdditionalAndContinueSimulation(worker.finishVehicles);
	}

    public void getVehiclesFromServiceCreateAdditionalAndContinueSimulation(Integer numberOfVehicles) {
//        System.out.printf("getVehiclesToNextStepAndContinueSimulation: step: %d, worker: %s, number: %d\n", step, name, numberOfVehicles);
	    if(numberOfVehicles>0){
            ArrayList<Vehicle> vehicles = new ArrayList<>();
            var vehiclesFromService = workerManager.createVehiclesService.vehiclesInAllWorkers.get(name);
            if(vehiclesFromService.size() >= numberOfVehicles){
                synchronized (vehiclesFromService){
                    var subVehicles = vehiclesFromService.stream().limit(numberOfVehicles).collect(Collectors.toList());
                    vehicles.addAll(subVehicles);
                    subVehicles.forEach(vehiclesFromService::remove);
                }
                vehicles.forEach(v -> v.timeRouteStart = timeNow);
                if(WorkerManager.printStream) System.out.printf("I GET FROM SERVICE: %d VEHICLES\n",vehicles.size());
                //MAYBE HERE ADDED THIS ADDITIONAL PARAMETERS
            } else{
                synchronized (vehiclesFromService){
                    vehicles.addAll(vehiclesFromService);
                    vehiclesFromService.clear();
                }
                int toCreate = numberOfVehicles - vehicles.size();
                if(WorkerManager.printStream) System.out.printf("I GET FROM SERVICE: %d VEHICLES BUT, I HAVE TO CREATE ADDITIONALLY %d VEHICLES, FOR WORKER %s, AND STEP: %d\n",vehicles.size(), toCreate, name, step);
                vehicles.forEach(v -> v.timeRouteStart = timeNow);
                trafficNetwork.createInternalNonPublicVehicles(toCreate, timeNow);
            }
            trafficNetwork.vehicles.addAll(vehicles);
            vehicles.forEach(v -> trafficNetwork.parkOneVehicle(v, true, v.timeRouteStart));
            trafficNetwork.newVehiclesSinceLastReport.addAll(vehicles);
        }

    }
}
