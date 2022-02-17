package processor.server;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.*;

import common.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import osm.OSM;
import processor.communication.*;
import processor.communication.message.*;
import processor.server.gui.GUI;
import processor.util.ServerTimings;
import processor.worker.WorkerManager;
import traffic.road.GridCell;
import traffic.road.Node;
import traffic.road.RoadNetwork;
import traffic.road.RoadUtil;

import javax.management.*;
import javax.print.attribute.HashAttributeSet;

import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;

/**
 * This class can do: 1) loading and distributing simulation configuration; 2)
 * balancing workload between workers; 3) instructing workers to perform tasks
 * (if server-based synchronization is enabled); 4) visualizing simulation; 5)
 * collecting results from workers; 6) writing results to files.
 *
 * This class can be run as Java application.
 */
public class Server implements MessageHandler, Runnable {
	IncomingConnectionBuilder connectionBuilder;

    private boolean connectedOnlyWm = true;
	public RoadNetwork roadNetwork;
    private HashMap<String, WorkerMeta> workerMetasByName = new HashMap<>();
    HashMap<String, ArrayList<WorkerMeta>> workerMetasInWorkerManager = new HashMap<>();
	ArrayList<WorkerManagerMeta> workerManagerMetas = new ArrayList<>();
	ArrayList<WorkerMeta> workerMetas = new ArrayList<>();
	int step = 0;// Time step in the current simulation
	GUI gui;
	FileOutput fileOutput = new FileOutput();
	public boolean isSimulating = false;// Whether simulation is running, i.e., it is not paused or stopped
	int numInternalNonPubVehiclesAtAllWorkers = 0;
	int numInternalTramsAtAllWorkers = 0;
	int numInternalBusesAtAllWorkers = 0;
	long timeStamp = 0;
	//	long initStartTimeStamp = 0;// Start time of Server initialization, utilized for calculating time of whole process, [NOTE: it is assumed that simulation is run only once ]
//	long setupStartTimeStamp = 0;// Start time of Workers setup, contains times of creation vehicles and its routes, ...
//	long fileDumpingStartTimeStamp = 0;// Start time of saving simulation metrics result into files
	long printRuntimeStatusTimeStamp = 0;
	double simulationWallTime = 0;// Total time length spent on simulation
	//	double initWallTime = 0;// Total time length spent on initialization by Server [setupNewSim()]
//	double setupWallTime = 0;// Total time length spent on setup by all Workers
	ScriptLoader scriptLoader = new ScriptLoader();
	int totalNumWwCommChannels = 0;// Total number of communication channels between workers. A worker has two
	// channels with a neighbor worker, one for sending and one for receiving.
	ArrayList<Node> nodesToAddLight = new ArrayList<>();
	ArrayList<Node> nodesToRemoveLight = new ArrayList<>();
	int numTrajectoriesReceived = 0;// Number of complete trajectories received from workers
	int numVehiclesCreatedDuringSetup = 0;// For updating setup progress on GUI
	int numVehiclesNeededAtStart = 0;// For updating setup progress on GUI
	double aggregatedVehicleCountInOneSimulation = 0.0;
	double aggregatedVehicleTravelSpeedInOneSimulation = 0.0;
	Server server;
	Scanner sc = new Scanner(System.in);
	boolean isOpenForNewWorkers = false;
	boolean isOpenForNewManWorkers = true;
	int trafficReportCounter = 0;
	ArrayList<Message_WS_TrafficReport> receivedTrafficReportCache = new ArrayList<>();
	ArrayList<SerializableRoute> routesForOutput = new ArrayList<SerializableRoute>();
	HashMap<String, TreeMap<Double, double[]>> trajectoriesForOutput = new HashMap<String, TreeMap<Double, double[]>>();
	HashMap<String, Double> travelTimesForOutput = new HashMap<String, Double>();
	public static int nrWorkers = 0;
	public static boolean oneSimulation = true;
	public static boolean defaultSimulation = true;
	public static String scriptPath = "";

	final ServerTimings serverTimings = new ServerTimings();

	Runtime runtime = Runtime.getRuntime();
	MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
	java.lang.management.ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
	int maxPeakT = 0;
	MessageProcessor messageProcessor;
	private static Logger serverLogger;
	static String name = "Server";
	private int setupDoneNumber = 0;
	private AsyncThreadedMsgSenderToWorker asyncThreadedMsgSenderToWorker;
    private AsyncThreadedMsgSenderToWorkerManager asyncThreadedMsgSenderToWorkerManager;
	int dumpingTrajDoneCounter = 0;
    private int numSetupDoneWorkerManagers = 0;
    public double totalLanesLength = 0;

	public static void main(final String[] args) {

		System.setProperty("logFilename", name);
		System.setProperty("logNodeIP", "");

		serverLogger = LogManager.getLogger("ServerLogger");
		serverLogger.error("TEST error test message from SERVER");
		serverLogger.info("Server is starting");

		if (processCommandLineArguments(args)) {
			try{
			    new Server().run();
            } catch (Exception e) {
                System.exit(1000);
            }

			MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
			Object attribute2 = null;
			Object attribute = null;
			try {
				attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
				attribute2 = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
			} catch (MBeanException e) {
				e.printStackTrace();
				serverLogger.error("Server MBeanException while accessing MBeanException bean", e);
			} catch (Exception e) {
				e.printStackTrace();
				serverLogger.error("Server Exception while reading MBeanException bean", e);
			}
			System.out.println("SERVER Total memory: "+ Long.parseLong(attribute.toString()) / 1024 /1024 +"MB");
			System.out.println("SERVER Free  memory: "+ Long.parseLong(attribute2.toString()) / 1024 /1024 +"MB");

			MemoryUsage MU = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
			String strHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
					"used=" + (MU.getUsed() >> 10) + "K| " +
					"committed=" + (MU.getCommitted() >> 10) + "K| " +
					"max=" + (MU.getMax() >> 10) + "K}";
			MU = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
			String strNonHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
					"used=" + (MU.getUsed() >> 10) + "K| " +
					"committed=" + (MU.getCommitted() >> 10) + "K| " +
					"max=" + (MU.getMax() >> 10) + "K}";

			/* freeMemory()    Total amount of free memory available to the JVM */
			/* .maxMemory()    Maximum amount of memory the JVM will attempt to use */
			/* totalMemory()   Total memory currently in use by the JVM */
			Runtime runtime = Runtime.getRuntime();

			java.lang.management.ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
			System.out.println("[SERVER][START] cpus=" + runtime.availableProcessors() +
					", activeT: " + threadMXBean.getThreadCount() +
					", peakT: " + threadMXBean.getPeakThreadCount() +
					", totalStartedT: " + threadMXBean.getTotalStartedThreadCount() +
					", cpus2=" + getOperatingSystemMXBean().getAvailableProcessors() +
					", loadAvg=" + getOperatingSystemMXBean().getSystemLoadAverage() +
					", P_CpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getProcessCpuLoad() +
					", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad() +
					", HeapBean=" + strHeapMU +
					", NonHeapBean=" + strNonHeapMU +
					", MaxMem=" + ((double)runtime.maxMemory()/(1024*1024)) +
					", TotalMem=" + ((double)runtime.totalMemory()/(1024*1024)) +
					", FreeMem=" + ((double)runtime.freeMemory()/(1024*1024))+
					", Total_OS: "+ Long.parseLong(attribute.toString()) / 1024 /1024 +"MB" +
					", Free_OS: "+ Long.parseLong(attribute2.toString()) / 1024 /1024 +"MB");


		} else {
			System.out.println("There is an error in command line parameter. Program exits.");
			serverLogger.warn("There is an error in command line parameter. Program exits.");
		}
	}

	private void logMachineState(){
		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
		Object attribute2 = null;
		Object attribute = null;
		try {
			attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
			attribute2 = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
		} catch (MBeanException e) {
			e.printStackTrace();
			serverLogger.error("Server MBeanException while accessing MBeanException bean", e);
		} catch (Exception e) {
			e.printStackTrace();
			serverLogger.error("Server Exception while reading MBeanException bean", e);
		}

		MemoryUsage MU = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		String strHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
				"used=" + (MU.getUsed() >> 10) + "K| " +
				"committed=" + (MU.getCommitted() >> 10) + "K| " +
				"max=" + (MU.getMax() >> 10) + "K}";

		MU = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
		String strNonHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
				"used=" + (MU.getUsed() >> 10) + "K| " +
				"committed=" + (MU.getCommitted() >> 10) + "K| " +
				"max=" + (MU.getMax() >> 10) + "K}";
		serverLogger.info("[LOG_MACHINE_STATE] cpus=" + runtime.availableProcessors() +
				", activeT: " + threadMXBean.getThreadCount() +
				", peakT: " + threadMXBean.getPeakThreadCount() +
				", totalStartedT: " + threadMXBean.getTotalStartedThreadCount() +
				", cpus2=" + getOperatingSystemMXBean().getAvailableProcessors() +
				", loadAvg=" + getOperatingSystemMXBean().getSystemLoadAverage() +
				", P_CpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getProcessCpuLoad() +
				", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad() +
				", HeapBean=" + strHeapMU +
				", NonHeapBean=" + strNonHeapMU +
				", MaxMem=" + ((double)runtime.maxMemory()/(1024*1024)) +
				", TotalMem=" + ((double)runtime.totalMemory()/(1024*1024)) +
				", FreeMem=" + ((double)runtime.freeMemory()/(1024*1024))+
				", Total_OS: "+ Long.parseLong(attribute.toString()) / 1024 /1024 +"MB" +
				", Free_OS: "+ Long.parseLong(attribute2.toString()) / 1024 /1024 +"MB");
	}

	static boolean processCommandLineArguments(final String[] args) {
		try {
			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
					case "-gui":
						Settings.isVisualize = Boolean.parseBoolean(args[i + 1]);
						break;
					case "-n":
						nrWorkers = Integer.parseInt(args[i + 1]);
						break;
					case "-script":
						scriptPath = args[i+1];
						defaultSimulation = false;
						break;
                    case "-grid_graph":
                        Settings.partitionType = "GridGraph";
                        break;
                    case "-wm":
                        Settings.numManWorkers = Integer.parseInt(args[i+1]);
				}
			}
		}catch (final Exception e) {
			return false;
		}
		return true;
	}

    void addWorkersFromWorkerManager(final Message_WM_FinishCreateWorkers received){
        int index =0;
        workerMetasInWorkerManager.put(received.workerManagerName, new ArrayList<>());
        for (SerializableWorkerInitData w: received.workers){
            final WorkerMeta worker = new WorkerMeta(received.workerManagerName, index, w.name, w.address, w.port);
            workerMetas.add(worker);
            workerMetasByName.put(worker.name, worker);
            workerMetasInWorkerManager.get(received.workerManagerName).add(worker);
            System.out.println(workerMetas.size() + "/" + Settings.numWorkers + " workers connected.");
        }
        if (workerMetas.size() == Settings.numWorkers) {
            //asyncThreadedMsgSenderToWorker = new AsyncThreadedMsgSenderToWorker(workerMetas);
            isOpenForNewWorkers = false;// No need for more workers
            acceptSimStartFromConsole();
        }
    }

	/**
	 * Adds a new worker unless simulation is running or the required number of
	 * workers is reached.
	 */
	void addWorker(final Message_WS_Join received) {
		final WorkerMeta worker = new WorkerMeta(received.workerName, received.workerAddress, received.workerPort, name);
		if (isAllWorkersAtState(WorkerState.NEW) && Settings.numWorkers > workerMetas.size()) {
            workerMetas.add(worker);
            workerMetasByName.put(worker.name, worker);
			System.out.println(workerMetas.size() + "/" + Settings.numWorkers + " workers connected.");
			if (Settings.isVisualize) {
				gui.updateNumConnectedWorkers(workerMetas.size());
				if (workerMetas.size() == Settings.numWorkers) {
					gui.getReadyToSetup();
				}
			} else {
				if (workerMetas.size() == Settings.numWorkers) {
					asyncThreadedMsgSenderToWorker = new AsyncThreadedMsgSenderToWorker(workerMetas);
					isOpenForNewWorkers = false;// No need for more workers
                    acceptSimStartFromConsole();
				}

			}
		} else {
			final Message_SW_KillWorker msd = new Message_SW_KillWorker(Settings.isSharedJVM);
			worker.send(msd);
		}
	}

    /**
     * Adds a new worker unless simulation is running or the required number of
     * workers is reached.
     */
    void addWorkerManager(final Message_WM_Join received) {
        final WorkerManagerMeta workerManager = new WorkerManagerMeta(received.workerManagerName, received.workerManagerAddress, received.workerManagerPort, name);
        if ( Settings.numManWorkers > workerManagerMetas.size()) {
            workerManagerMetas.add(workerManager);
            System.out.println(workerManagerMetas.size() + "/" + Settings.numManWorkers+ " workers Manager connected.");
            if (workerManagerMetas.size() == Settings.numManWorkers) {
                acceptSimScriptFromConsole();// Let user input simulation script path
                asyncThreadedMsgSenderToWorkerManager = new AsyncThreadedMsgSenderToWorkerManager(workerManagerMetas);
                isOpenForNewManWorkers = false;// No need for more worker managers
                setupNewSimWithWorkerManagers();
                //isOpenForNewWorkers = true;
                //System.out.println("Please launch workers now.");

            }

        } else {
            //HERE PROBABLY TO CHANGE
            final Message_SW_KillWorker msd = new Message_SW_KillWorker(Settings.isSharedJVM);
            workerManager.send(msd);
        }
    }


	/*
	 * Start a server-less simulation.
	 */
	void askWorkersProceedWithoutServer() {
		timeStamp = System.nanoTime();
		serverTimings.startSendingStart();
		if (Settings.parallelMessageSending) {
//			if (workerMetas.size() > 0)
			asyncThreadedMsgSenderToWorkerManager.sendOutToAllWorkersManagers(SendMessageType.Message_SW_Serverless_Start, step);
		} else {
			final Message_SW_Serverless_Start message = new Message_SW_Serverless_Start(step);
			for (final WorkerManagerMeta workerManager : workerManagerMetas) {
                workerManager.send(message);
                workerMetasInWorkerManager.get(workerManager.name).forEach(w -> w.setState(WorkerState.SERVERLESS_WORKING));
			}
		}
		serverTimings.resetSimulationTimer();
		serverTimings.stopSendingStart(System.out);
		printRuntimeStatusTimeStamp = System.nanoTime();
	}
	/*
	 * Ask workers to transfer vehicles information to fellow workers in
	 * server-based synchronization mode.
	 */

	void askWorkersShareTrafficDataWithFellowWorkers() {

		// Increment step
		step++;
		timeStamp = System.nanoTime();
		serverTimings.resetSimulationTimer();

		// Ask all workers to share data with fellow
		for (final WorkerMeta worker : workerMetas) {
			worker.setState(WorkerState.SHARING_STARTED);
		}
		for (final WorkerMeta worker : workerMetas) {
			worker.send(new Message_SW_ServerBased_ShareTraffic(step));
		}
	}

	/*
	 * Ask workers to update traffic in their corresponding work areas for one time
	 * step. This is called after workers exchange traffic information with their
	 * neighbors in server-based simulation.
	 */
	synchronized void askWorkersSimulateOneStep() {
		boolean isNewNonPubVehiclesAllowed = numInternalNonPubVehiclesAtAllWorkers < Settings.numGlobalRandomBackgroundPrivateVehicles
				? true
				: false;
		boolean isNewTramsAllowed = numInternalTramsAtAllWorkers < Settings.numGlobalBackgroundRandomTrams ? true
				: false;
		boolean isNewBusesAllowed = numInternalBusesAtAllWorkers < Settings.numGlobalBackgroundRandomBuses ? true
				: false;

		// Clear one-step vehicle counts from last step
		numInternalNonPubVehiclesAtAllWorkers = 0;
		numInternalTramsAtAllWorkers = 0;
		numInternalBusesAtAllWorkers = 0;

		for (final WorkerMeta worker : workerMetas) {
			worker.setState(WorkerState.SIMULATING);
		}
		final Message_SW_ServerBased_Simulate message = new Message_SW_ServerBased_Simulate(isNewNonPubVehiclesAllowed,
				isNewTramsAllowed, isNewBusesAllowed, UUID.randomUUID().toString());
		for (final WorkerMeta worker : workerMetas) {
			worker.send(message);
		}
	}

	void buildGui() {
		if (gui != null) {
			gui.dispose();
		}
		final GUI newGUI = new GUI(this);
		gui = newGUI;
		gui.setVisible(true);
	}

	public void changeMap() {
		Settings.listRouteSourceWindowForInternalVehicle.clear();
		Settings.listRouteDestinationWindowForInternalVehicle.clear();
		Settings.listRouteSourceDestinationWindowForInternalVehicle.clear();
		Settings.isNewEnvironment = true;

		if (Settings.inputOpenStreetMapFile.length() > 0) {
			final OSM osm = new OSM();
			osm.processOSM(Settings.inputOpenStreetMapFile, true, "SERVER");
			Settings.isBuiltinRoadGraph = false;
			// Revert to built-in map if there was an error when converting new map
			if (Settings.roadGraph.length() == 0) {
				try{
					throw new Exception("MAP NOT PARSED CORRECTLY by changeMap() method");
				}catch (Exception e){
					System.err.println("[SERVER][ERROR] MAP WAS NOT PARSED CORRECTLY BY SERVER!!!");
					e.printStackTrace();
					serverLogger.error("[SERVER][ERROR] MAP WAS NOT PARSED CORRECTLY BY SERVER!!!", e);
				}
				Settings.roadGraph = RoadUtil.importBuiltinRoadGraphFile();
				Settings.isBuiltinRoadGraph = true;
			}
			System.out.println("[OSM][SERVER] Road network graph built based on OSM file: "
					+ Settings.inputOpenStreetMapFile);
//			osm.dumpMapToFileDebugMethod("ServerRoads.txt");
		} else {
			Settings.roadGraph = RoadUtil.importBuiltinRoadGraphFile();
			Settings.isBuiltinRoadGraph = true;
		}

		roadNetwork = new RoadNetwork();// Build road network based on new road graph
	}

	/**
	 * Change simulation speed by setting pause time after doing a step at all
	 * workers. Note that this will affect simulation time. By default there is no
	 * pause time between steps.
	 */
	public void changeSpeed(final int pauseTimeEachStep) {
		Settings.pauseTimeBetweenStepsInMilliseconds = pauseTimeEachStep;
		final Message_SW_ChangeSpeed message = new Message_SW_ChangeSpeed(Settings.pauseTimeBetweenStepsInMilliseconds);
		for (final WorkerMeta worker : workerMetas) {
			worker.send(message);
		}
	}

	// TODO it is useful function to determine if node of vehicle is bounded to worker
	public WorkerMeta getWorkerAtRouteStart(final Node routeStartNode) {
		for (final WorkerMeta worker : workerMetas) {
			if (worker.workarea.workCells.contains(routeStartNode.gridCell)) {
				return worker;
			}
		}
		return null;
	}

	boolean isAllWorkersAtState(final WorkerState state) {
		int count = 0;
		for (final WorkerMeta w : workerMetas) {
			if (w.isEqual(state)) {
				count++;
			}
		}

		if (count == workerMetas.size()) {
			return true;
		} else {
			return false;
		}

	}

	public void killConnectedWorkers() {
		serverLogger.info(" [Server] Killing workers...");

		if (Settings.parallelMessageSending) {
			if (workerManagerMetas.size()>0)
				asyncThreadedMsgSenderToWorkerManager.sendOutToAllWorkersManagers(SendMessageType.Message_SW_KillWorker, step);
		}
		else{
			final Message_SW_KillWorker msd = new Message_SW_KillWorker(Settings.isSharedJVM);
			for (final WorkerManagerMeta workerManager : workerManagerMetas) {
				workerManager.send(msd);
			}
		}

		workerMetas.clear();
		Settings.isNewEnvironment = true;
	}

	public void pauseSim() {
		isSimulating = false;
		if (!Settings.isServerBased) {
			final Message_SW_Serverless_Pause message = new Message_SW_Serverless_Pause(step);
			for (final WorkerMeta worker : workerMetas) {
				worker.send(message);
			}
		}
	}

	private synchronized void incrementDoneWorkerManagers(){
        numSetupDoneWorkerManagers++;
    }
	/**
	 * Process received message sent from worker. Based on the received message,
	 * server can update GUI, decide whether to do next time step or finish
	 * simulation and instruct the workers what to do next.
	 */
	@Override
	synchronized public void processReceivedMsg(final Object message) {

		if (message instanceof Message_WS_Join) {
            System.out.println("GET MESSAGE WS JOIN NUMBER: " + workerMetas.size());
			final Message_WS_Join messageToProcess = (Message_WS_Join) message;
			serverLogger.debug("[Msg_WS_Join] - name: "+ messageToProcess.workerName
					+ ", address: "+ messageToProcess.workerAddress +", port: "+ messageToProcess.workerPort
					+ ", Q: " + messageProcessor.getQueueSize());

			if (isOpenForNewWorkers)
				addWorker((Message_WS_Join) message);

			if (workerMetas.size()%24 == 0)
				serverLogger.info("[Msg_WS_Join] - l_name="+ messageToProcess.workerName
						+ ", l_addr="+ messageToProcess.workerAddress +", l_port="+ messageToProcess.workerPort
						+ ", Q=" + messageProcessor.getQueueSize() + ", #alreadyJoined=" + workerMetas.size());

		} else if (message instanceof Message_WM_Join) {
            final Message_WM_Join messageToProcess = (Message_WM_Join) message;
            serverLogger.debug("[Msg_WM_Join] - name: "+ messageToProcess.workerManagerName
                    + ", address: "+ messageToProcess.workerManagerAddress +", port: "+ messageToProcess.workerManagerPort
                    + ", Q: " + messageProcessor.getQueueSize());
            if (isOpenForNewManWorkers)
                addWorkerManager(messageToProcess);

        } else if (message instanceof Message_WM_FinishCreateWorkers) {
            final Message_WM_FinishCreateWorkers messageToProcess = (Message_WM_FinishCreateWorkers) message;

            if(isOpenForNewWorkers){
                addWorkersFromWorkerManager(messageToProcess);
            }


        } else if (message instanceof Message_WM_SetupDone) {
            final Message_WM_SetupDone messageToProcess = (Message_WM_SetupDone) message;
            serverLogger.debug("[Msg_WM_SetupDone] - name: "+ messageToProcess.workerManagerName
                    + ", Q: " + messageProcessor.getQueueSize());
            incrementDoneWorkerManagers();
            if(numSetupDoneWorkerManagers == Settings.numManWorkers){
                //HERE SEND CREATEWORKERS
                sendCreateWorkerMessageToWorkerManagers();
                isOpenForNewWorkers = true;
            }


        } else if (message instanceof Message_WS_SetupCreatingVehicles) {
			serverLogger.debug("[Msg_WS_SetupCreatingVehicles] - NOT APPLICABLE");

			numVehiclesCreatedDuringSetup += ((Message_WS_SetupCreatingVehicles) message).numVehicles;

			double createdVehicleRatio = (double) numVehiclesCreatedDuringSetup / numVehiclesNeededAtStart;
			if (createdVehicleRatio > 1) {
				createdVehicleRatio = 1;
			}

			if (Settings.isVisualize) {
				gui.updateSetupProgress(createdVehicleRatio);
			}
		} else if (message instanceof Message_WS_SetupDone) {
			final Message_WS_SetupDone messageToProcess = (Message_WS_SetupDone) message;
			serverLogger.debug("[Msg_WS_SetupDone] - worker: "+ messageToProcess.workerName
					+ ", #fellows: "+ messageToProcess.numFellowWorkers
					+ ", Q: " + messageProcessor.getQueueSize());

			updateWorkerState(((Message_WS_SetupDone) message).workerName, WorkerState.READY);
			totalNumWwCommChannels += (((Message_WS_SetupDone) message).numFellowWorkers);
			if (totalNumWwCommChannels%100==0){
				maxPeakT = Integer.max(maxPeakT, threadMXBean.getPeakThreadCount());
				threadMXBean.resetPeakThreadCount();
				System.out.println("[SETUP_DONE][SERVER]: " + totalNumWwCommChannels +
						", activeT: " + threadMXBean.getThreadCount() +
						", peakT: " + threadMXBean.getPeakThreadCount() +
						", maxPeakT: " + maxPeakT +
						", totalStartedT: " + threadMXBean.getTotalStartedThreadCount());
			}

			setupDoneNumber++;
			if ((setupDoneNumber)%24 == 0)
				serverLogger.info("[Msg_WS_SetupDone] - "
						+ "Q=" + messageProcessor.getQueueSize()
						+ ", #setupDone: " + setupDoneNumber
						+ ", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad()
						+ ", WWconnections=" + totalNumWwCommChannels);


			if (isAllWorkersAtState(WorkerState.READY)) {
				if (Settings.isVisualize) {
					gui.stepToDraw = 0;
				}
				System.out.println("Settings: maxNumSteps: " + Settings.maxNumSteps +
						", random background vehicles: " + Settings.numGlobalRandomBackgroundPrivateVehicles +
						", trajectory scope: " + Settings.outputTrajectoryScope);
				serverLogger.info("[Msg_WS_SetupDone] - all workers done with setup!" +
						"   Settings: maxNumSteps: " + Settings.maxNumSteps +
						", random background vehicles: " + Settings.numGlobalRandomBackgroundPrivateVehicles +
						", trajectory scope: " + Settings.outputTrajectoryScope +
						", Q: " + messageProcessor.getQueueSize());

				//				setupWallTime = getSetupTime();
				serverLogger.info(" [Timer] End - stop setup timer.");
				serverTimings.stopSecondStage();
				logMachineState();

				serverLogger.info("SENDING - MSG_WS_START to all workers. Q: " + messageProcessor.getQueueSize());
				startSimulation();
				serverLogger.info("DONE - MSG_WS_START sent to all workers. Q: "+ messageProcessor.getQueueSize() +
						", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad());
			}
        } else if (message instanceof Message_WM_FinishSetupWorkers) {
            final Message_WM_FinishSetupWorkers messageToProcess = (Message_WM_FinishSetupWorkers) message;
            for(Message_WS_SetupDone setupDoneMessage: messageToProcess.setupDones){
                serverLogger.debug("[Msg_WS_SetupDone] - worker: "+ setupDoneMessage.workerName
                            + ", #fellows: "+ setupDoneMessage.numFellowWorkers
                            + ", Q: " + messageProcessor.getQueueSize());

                updateWorkerState(setupDoneMessage.workerName, WorkerState.READY);
                totalNumWwCommChannels += (setupDoneMessage.numFellowWorkers);

                setupDoneNumber++;
                if ((setupDoneNumber)%100 == 0)
                    serverLogger.info("[Msg_WS_SetupDone] - "
                            + "Q=" + messageProcessor.getQueueSize()
                            + ", #setupDone: " + setupDoneNumber
                            + ", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad()
                            + ", WWconnections=" + totalNumWwCommChannels);
            }

            if (isAllWorkersAtState(WorkerState.READY)) {
                if (Settings.isVisualize) {
                    gui.stepToDraw = 0;
                }
                System.out.println("Settings: maxNumSteps: " + Settings.maxNumSteps +
                        ", random background vehicles: " + Settings.numGlobalRandomBackgroundPrivateVehicles +
                        ", trajectory scope: " + Settings.outputTrajectoryScope);
                serverLogger.info("[Msg_WS_SetupDone] - all workers done with setup!" +
                        "   Settings: maxNumSteps: " + Settings.maxNumSteps +
                        ", random background vehicles: " + Settings.numGlobalRandomBackgroundPrivateVehicles +
                        ", trajectory scope: " + Settings.outputTrajectoryScope +
                        ", Q: " + messageProcessor.getQueueSize());

                //				setupWallTime = getSetupTime();
                serverLogger.info(" [Timer] End - stop setup timer.");
                serverTimings.stopSecondStage();
                logMachineState();

                serverLogger.info("SENDING - MSG_WS_START to all workers. Q: " + messageProcessor.getQueueSize());
                startSimulation();
                serverLogger.info("DONE - MSG_WS_START sent to all workers. Q: "+ messageProcessor.getQueueSize() +
                        ", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad());
            }

		} else if (message instanceof Message_WS_ServerBased_SharedMyTrafficWithNeighbor) {
			serverLogger.debug("[Msg_WS_ServerBased_SharedMyTrafficWithNeighbor] - Received");

			if (!isSimulating) {
				// No need to process the message if simulation was stopped
				return;
			}

			Message_WS_ServerBased_SharedMyTrafficWithNeighbor messageToProcess = (Message_WS_ServerBased_SharedMyTrafficWithNeighbor) message;

			for (final WorkerMeta w : workerMetas) {
				if (w.name.equals(messageToProcess.workerName) && (w.isEqual(WorkerState.SHARING_STARTED))) {
					updateWorkerState(messageToProcess.workerName, WorkerState.SHARED);
					if (isAllWorkersAtState(WorkerState.SHARED)) {
						askWorkersSimulateOneStep();
					}
					break;
				}
			}

		} else if (message instanceof Message_WM_TrafficReport) {

            final Message_WM_TrafficReport msg = (Message_WM_TrafficReport) message;
            for(final Message_WS_TrafficReport received: msg.trafficReports){
                serverLogger.debug("[Msg_WS_TrafficReport] - worker: "+ received.workerName
                        + ", step: "+ received.step
                        + ", Q: " + messageProcessor.getQueueSize());

                // Cache received reports
                receivedTrafficReportCache.add(received);

                // Output data from the reports
                processCachedReceivedTrafficReports();

                // Stop if max number of steps is reached in server-based mode
                if (Settings.isServerBased) {
                    updateWorkerState(received.workerName, WorkerState.FINISHED_ONE_STEP);
                    if (isAllWorkersAtState(WorkerState.FINISHED_ONE_STEP)) {
                        updateSimulationTime();
                        serverTimings.updateSimulationTimer();
                        if (step >= Settings.maxNumSteps) {
                            stopSim();
                        } else if (isSimulating) {
                            askWorkersShareTrafficDataWithFellowWorkers();
                        }
                    }
                } else {

                    // Update time step in server-less mode
                    if (received.step > step) {

                        step = received.step;

                    }
                }
            }

        }
        else if (message instanceof Message_WS_TrafficReport) {

			if (!isSimulating) {
				// No need to process the message if simulation was stopped
				return;
			}

			final Message_WS_TrafficReport received = (Message_WS_TrafficReport) message;
			serverLogger.debug("[Msg_WS_TrafficReport] - worker: "+ received.workerName
					+ ", step: "+ received.step
					+ ", Q: " + messageProcessor.getQueueSize());

			// Cache received reports
			receivedTrafficReportCache.add(received);

			// Output data from the reports
			processCachedReceivedTrafficReports();

			// Stop if max number of steps is reached in server-based mode
			if (Settings.isServerBased) {
				updateWorkerState(received.workerName, WorkerState.FINISHED_ONE_STEP);
				if (isAllWorkersAtState(WorkerState.FINISHED_ONE_STEP)) {
					updateSimulationTime();
					serverTimings.updateSimulationTimer();
					if (step >= Settings.maxNumSteps) {
						stopSim();
					} else if (isSimulating) {
						askWorkersShareTrafficDataWithFellowWorkers();
					}
				}
			} else {

				// Update time step in server-less mode
				if (received.step > step) {

					step = received.step;

				}
			}
		} else if (message instanceof Message_WM_Serverless_Complete) {
            if (!isSimulating) {
                // No need to process the message if simulation was stopped
                return;
            }
            final Message_WM_Serverless_Complete msg = (Message_WM_Serverless_Complete) message;
            for(final Message_WS_Serverless_Complete received: msg.serverlessCompletes){
                serverLogger.debug("[Msg_WS_Serverless_Complete] - worker: "+ received.workerName
                        + ", r_step: "+ received.step + ", c_step: " + step
                        + ", Q: " + messageProcessor.getQueueSize());

                if (received.step > step) {
                    step = received.step;
                }

                updateWorkerState(received.workerName, WorkerState.NEW);

                if (isAllWorkersAtState(WorkerState.NEW)) {
                    updateSimulationTime();
                    serverTimings.updateSimulationTimer();

                    serverLogger.info("All workers completed simulation. Q: " + messageProcessor.getQueueSize() +
                            ", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad());
                    stopSim();
                    serverLogger.info("Simulation is stopped. Q: " + messageProcessor.getQueueSize() +
                            ", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad());

                }
            }
        }
        else if (message instanceof Message_WS_Serverless_Complete) {

			if (!isSimulating) {
				// No need to process the message if simulation was stopped
				return;
			}
			final Message_WS_Serverless_Complete received = (Message_WS_Serverless_Complete) message;
			serverLogger.debug("[Msg_WS_Serverless_Complete] - worker: "+ received.workerName
					+ ", r_step: "+ received.step + ", c_step: " + step
					+ ", Q: " + messageProcessor.getQueueSize());

			if (received.step > step) {
				step = received.step;
			}

			updateWorkerState(received.workerName, WorkerState.NEW);

			if (isAllWorkersAtState(WorkerState.NEW)) {
				updateSimulationTime();
				serverTimings.updateSimulationTimer();

				serverLogger.info("All workers completed simulation. Q: " + messageProcessor.getQueueSize() +
						", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad());
				stopSim();
				serverLogger.info("Simulation is stopped. Q: " + messageProcessor.getQueueSize() +
						", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad());

			}
		} else if (message instanceof Message_WS_DumpingDone) {
			if (++dumpingTrajDoneCounter >= workerManagerMetas.size()) {
				serverLogger.info("[Message_WS_DumpingDone] - Received (" + dumpingTrajDoneCounter + " / " + workerMetas.size() + ") messages"
						+ ", Q: " + messageProcessor.getQueueSize());
				closeSim();
			}
			serverLogger.debug("[Message_WS_DumpingDone] - worker: "
					+ ", Q: " + messageProcessor.getQueueSize());
		}
	}

	synchronized void processCachedReceivedTrafficReports() {
		final Iterator<Message_WS_TrafficReport> iMessage = receivedTrafficReportCache.iterator();
		while (iMessage.hasNext()) {
			trafficReportCounter++;
			printServerState();

			final Message_WS_TrafficReport message = iMessage.next();
			// Update GUI
			if (Settings.isVisualize) {
				gui.updateObjectData(message.vehicles, message.trafficLights, message.workerName, workerMetas.size(),
						message.step);
			}
			// Build trajectories of vehicles based on the vehicle list
			if (Settings.outputTrajectoryScope != DataOutputScope.NONE) {
				double timeStamp = message.step / Settings.numStepsPerSecond;
				for (Serializable_GUI_Vehicle vehicle : message.vehicles) {
					if (Settings.outputTrajectoryScope == DataOutputScope.ALL
							|| (vehicle.isForeground && Settings.outputTrajectoryScope == DataOutputScope.FOREGROUND)
							|| (!vehicle.isForeground
							&& Settings.outputTrajectoryScope == DataOutputScope.BACKGROUND)) {
						String key=vehicle.id+"_"+vehicle.type;
						if (!trajectoriesForOutput.containsKey(key)) {
							trajectoriesForOutput.put(key, new TreeMap<Double, double[]>());
						}
						trajectoriesForOutput.get(key).put(timeStamp,
								new double[] { vehicle.latHead, vehicle.lonHead });
					}
				}
			}
			// Aggregate vehicle count and travel speed
			aggregatedVehicleCountInOneSimulation += message.totalNumVehicles;

			aggregatedVehicleTravelSpeedInOneSimulation += message.aggregatedTravelSpeedValues;
			// Store routes of new vehicles created since last report

			routesForOutput.addAll(message.newRoutesSinceLastReport);
			// Update for travel time output
			for (SerializableTravelTime record : message.travelTimes) {
				travelTimesForOutput.put(record.ID, record.travelTime);
			}
			// Increment vehicle counts
			numInternalNonPubVehiclesAtAllWorkers += message.numInternalNonPubVehicles;
			numInternalTramsAtAllWorkers += message.numInternalTrams;
			numInternalBusesAtAllWorkers += message.numInternalBuses;
			// Remove processed message
			iMessage.remove();
		}
	}

	void printServerState(){
		if (trafficReportCounter % (50*Settings.numWorkers) == 0) {
			Object attribute2 = null;
			Object attribute = null;
			try {
				attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
				attribute2 = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
			}catch (MBeanException e) {
				e.printStackTrace();
				serverLogger.error("Server MBeanException while accessing MBeanException bean", e);
			} catch (Exception e) {
				e.printStackTrace();
				serverLogger.error("Server Exception while reading MBeanException bean", e);
			}


			MemoryUsage MU = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
			String strNonHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
					"used=" + (MU.getUsed() >> 10) + "K| " +
					"committed=" + (MU.getCommitted() >> 10) + "K| " +
					"max=" + (MU.getMax() >> 10) + "K}";

			maxPeakT = Integer.max(maxPeakT, threadMXBean.getPeakThreadCount());

			/* freeMemory()    Total amount of free memory available to the JVM */
			/* .maxMemory()    Maximum amount of memory the JVM will attempt to use */
			/* totalMemory()   Total memory currently in use by the JVM */
			System.out.println("Server: Step=" + step +
					", reportCounter=" + trafficReportCounter +
					", sec50steps=" + ((System.nanoTime() - printRuntimeStatusTimeStamp)/1.0E9)+
					", cpus=" + runtime.availableProcessors() +
					", activeT= " + threadMXBean.getThreadCount() +
					", peakT= " + threadMXBean.getPeakThreadCount() +
					", maxPeakT= " + maxPeakT +
					", totalStartedT= " + threadMXBean.getTotalStartedThreadCount() +
					", Total_OS= "+ Long.parseLong(attribute.toString()) / 1024 /1024 +"MB" +
					", Free_OS= "+ Long.parseLong(attribute2.toString()) / 1024 /1024 +"MB"+
					", loadAvg=" + getOperatingSystemMXBean().getSystemLoadAverage() +
					", P_CpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getProcessCpuLoad() +
					", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad() +
//								", HeapBean=" + strHeapMU +
					", NonHeapBean=" + strNonHeapMU +
					", cpus2=" + getOperatingSystemMXBean().getAvailableProcessors() +
					", MaxMem=" + ((double)runtime.maxMemory()/(1024*1024)) +
					", TotalMem=" + ((double)runtime.totalMemory()/(1024*1024)) +
					", FreeMem=" + ((double)runtime.freeMemory()/(1024*1024)));
			printRuntimeStatusTimeStamp = System.nanoTime();
		}
	}


	@Override
	public void run() {
		if (serverLogger == null){
			System.setProperty("logFilename", name);
			System.setProperty("logNodeIP", "");

			serverLogger = LogManager.getLogger("ServerLogger");
		}
		// Set initialization starting timeStamp
//		initStartTimeStamp = System.nanoTime();
		logMachineState();

		serverLogger.info(" [Timer] Start Initialization timer and whole processs timer.");
		serverTimings.startFirstStage();

		// Load default road network
		Settings.roadGraph = RoadUtil.importBuiltinRoadGraphFile();
		roadNetwork = new RoadNetwork();

		// Start GUI or load simulation configuration without GUI
		if (Settings.isVisualize) {
			buildGui();
		} else {
			acceptInitialConfigFromConsole();
		}

		logMachineState();
		serverLogger.info("Initializing MessageProcessor for server...");
		// Prepare message processing units
		messageProcessor = new MessageProcessor("SERVER", 120, this);
		messageProcessor.runAllProccessors();
		serverLogger.info("MessageProcessor started: " + messageProcessor.toString());

		logMachineState();

		serverLogger.info("Initializing IncomingConnectionBuilder for server...");
		// Prepare to receive connection request from workers
		connectionBuilder = new IncomingConnectionBuilder(messageProcessor, Settings.serverListeningPortForWorkers, this, null);
		connectionBuilder.setName("[" + messageProcessor.getOwnerName() + "][ConnectionBuilder]");
		connectionBuilder.start();
		serverLogger.info("IncomingConnectionBuilder started: " + connectionBuilder.toString());
		logMachineState();
	}
    //HERE CHANGE TO ACCEPT WORKERMANAGER FIRST
	void acceptInitialConfigFromConsole() {
		// Let user input number of workers
		if(nrWorkers == 0){
			System.out.println("Please specify the number of workers.");
			Settings.numWorkers = Integer.parseInt(sc.nextLine());
			while (Settings.numWorkers <= 0) {
				System.out.println("Please specify the number of workers.");
				Settings.numWorkers = Integer.parseInt(sc.nextLine());
			}
		} else {
			Settings.numWorkers = nrWorkers;
            System.out.println(String.format("Nr of worker managers: %d", Settings.numManWorkers));
			System.out.println(String.format("Nr of workers: %d", nrWorkers));
		}
        if(defaultSimulation){
            System.out.println("Please specify the partitioning type [GridGraph, Space-grid].");
            Settings.partitionType = sc.nextLine();
            while (!(Settings.partitionType.equals("GridGraph") || Settings.partitionType.equals("Space-grid")) ) {
                System.out.println("Please specify the partitioning type [GridGraph, Space-grid].");
                Settings.partitionType = sc.nextLine();
            }
        }

		// Kill all connected workers
		killConnectedWorkers();
		// Inform user next step
		System.out.println("Please launch worker managers now.");
	}

	void acceptSimStartCommandFromConsole() {

		if(defaultSimulation){
			System.out.println("Ready to simulate. Start (y/n)?");
			String choice = sc.nextLine();
			if (choice.equals("y") || choice.equals("Y")) {
				startSimulationFromLoadedScript();
			} else if (choice.equals("n") || choice.equals("N")) {
				System.out.println("Quit system.");
				killConnectedWorkers();
				System.exit(0);
			} else {
				System.out.println("Ready to simulate. Start (y/n)?");
			}
		} else {
			if (oneSimulation){
				startSimulationFromLoadedScript();
				oneSimulation = false;
			} else {
				System.out.println("Quit system.");
				killConnectedWorkers();
				System.exit(0);
			}
		}
	}

	void acceptConsoleCommandAtSimEnd() {
		if(defaultSimulation){
			System.out.println("Simulations are completed. Exit (y/n)?");
			String choice = sc.nextLine();
			if (choice.equals("y") || choice.equals("Y")) {
				// Kill all connected workers
				System.out.println("Quit system.");
				killConnectedWorkers();
				System.exit(0);
			} else if (choice.equals("n") || choice.equals("N")) {
				acceptSimScriptFromConsole();
			} else {
				System.out.println("Simulations are completed. Exit (y/n)?");
			}
		} else{
			System.out.println("Simulations are completed.");
			System.out.println("Quit system.");
			killConnectedWorkers();
			System.exit(0);
		}

	}

    void acceptSimStartFromConsole() {
        if (workerMetas.size() == Settings.numWorkers) {
            acceptSimStartCommandFromConsole();
        }
    }

	void acceptSimScriptFromConsole() {
		if(defaultSimulation){
			System.out.println("Please specify the simulation script path.");
			Settings.inputSimulationScript = sc.nextLine();
			while (!scriptLoader.loadScriptFile()) {
				System.out.println("Please specify the simulation script path.");
				Settings.inputSimulationScript = sc.nextLine();
			}
		} else {
			Settings.inputSimulationScript = scriptPath;
			scriptLoader.loadScriptFile();
		}
        if (scriptLoader.retrieveOneSimulationSetup()) {
            changeMap();
        }

	}

	public void resumeSim() {
		isSimulating = true;
		if (Settings.isServerBased) {
			System.out.println("Resuming server-based simulation...");
			askWorkersShareTrafficDataWithFellowWorkers();
		} else {
			System.out.println("Resuming server-less simulation...");
			final Message_SW_Serverless_Resume message = new Message_SW_Serverless_Resume(step);
			for (final WorkerMeta worker : workerMetas) {
				worker.send(message);
			}
		}
	}

	/**
	 * Update node lists for nodes where traffic light needs to be added or removed.
	 * The lists will be sent to worker during setup.
	 */
	public void setLightChangeNode(final Node node) {
		node.light = !node.light;
		nodesToAddLight.remove(node);
		nodesToRemoveLight.remove(node);
		if (node.light) {
			nodesToAddLight.add(node);
		} else {
			nodesToRemoveLight.add(node);
		}
	}

	ArrayList<double[]> setRouteSourceDestinationWindow(final ArrayList<Serializable_GPS_Rectangle> sList) {
		final ArrayList<double[]> list = new ArrayList<>();
		for (final Serializable_GPS_Rectangle sgr : sList) {
			// Skip the zero item when the list does not have meaningful items
			if ((sgr.minLon == 0) && (sgr.maxLat == 0) && (sgr.maxLon == 0) && (sgr.minLat == 0)) {
				continue;
			}
			list.add(new double[] { sgr.minLon, sgr.maxLat, sgr.maxLon, sgr.minLat });
		}
		return list;
	}

	public void sendCreateWorkerMessageToWorkerManagers(){
        if (Settings.parallelMessageSending) {
            //serverLogger.info(" [Timer] Start sending setup to workers timer - Parallel.");
            asyncThreadedMsgSenderToWorkerManager.sendOutToAllCreateWorkerToWorkerManagers();

//			asyncThreadedMsgSender.sendOutToAllWorkers(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal, SendMessageType.Message_SW_Setup);
        }
        else{
            //serverLogger.info(" [Timer] Start sending setup to workers timer - Sequential.");
            for (int i = 0; i < workerManagerMetas.size(); i++) {
                if (i % 20 == 0) {
                    System.out.println("[Server][SENDING_CONFIG] HeapTotal: " + runtime.totalMemory() + ", HeapFree: " + runtime.freeMemory() +
                            ", activeT: " + threadMXBean.getThreadCount() +
                            ", peakT: " + threadMXBean.getPeakThreadCount() +
                            ", totalStartedT: " + threadMXBean.getTotalStartedThreadCount() +
                            ", WORKERS_AMOUNT: " + i);
                }
                final WorkerManagerMeta workerManager = workerManagerMetas.get(i);
                workerManager.send(new Message_SWM_CreateWorkers());
            }
        }
    }

	/**
	 * Resets dynamic fields and sends simulation configuration to workers. The
	 * workers will set up simulation environment upon receiving the configuration.
	 */

    public void setupNewSimWithWorkerManagers(){
        // Reset worker managers
        for (final WorkerManagerMeta workerManager : workerManagerMetas) {
            workerManager.setState(WorkerState.NEW);
        }

        if (Settings.isNewEnvironment) {
            roadNetwork.buildGrid();
        }

        final GridCell[][] grid = roadNetwork.grid;

        for (int i = 0; i < Settings.numGridRows; i++) {
            for (int j = 0; j < Settings.numGridCols; j++) {
                totalLanesLength += grid[i][j].laneLength;
            }
        }

        System.out.println("Map road Graph length: " + Settings.roadGraph.length() +
                ", Nodes: " + roadNetwork.nodes.size() +
                ", Edges: " + roadNetwork.edges.size() +
                ", Lanes: " + roadNetwork.lanes.size() +
                ", LaneLengthOfWholeMap: " + totalLanesLength +
                ", minLat: " + roadNetwork.minLat + ", minLon: " + roadNetwork.minLon +
                ", maxLat: " + roadNetwork.maxLat + ", maxLon: " + roadNetwork.maxLon +
                ", mapWidth: " + roadNetwork.mapWidth + ", mapHeight: " + roadNetwork.mapHeight);

        int trafficHashVal = roadNetwork.generateHash();
        System.out.println("Server hash of map: " + trafficHashVal);

        serverLogger.info(" [Timer] End - Stop Initialization timer and whole processs timer.");
        serverLogger.info(" [Timer] Start setup timer.");
        serverTimings.stopFirstAndStartSecStage();

        serverTimings.startSendingSetup();
        if (Settings.parallelMessageSending) {
            serverLogger.info(" [Timer] Start sending setup to workers timer - Parallel.");
            Message_SWM_Setup template = new Message_SWM_Setup(nodesToAddLight, nodesToRemoveLight, trafficHashVal);
            int templateHash = template.generateHash();

            asyncThreadedMsgSenderToWorkerManager.sendOutToAllWorkerManagers(template, SendMessageType.Message_SWM_Setup, templateHash);
//			asyncThreadedMsgSender.sendOutToAllWorkers(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal, SendMessageType.Message_SW_Setup);
        }
        else{
            serverLogger.info(" [Timer] Start sending setup to workers timer - Sequential.");
            for (int i = 0; i < workerManagerMetas.size(); i++) {
                if (i % 20 == 0) {
                    System.out.println("[Server][SENDING_CONFIG] HeapTotal: " + runtime.totalMemory() + ", HeapFree: " + runtime.freeMemory() +
                            ", activeT: " + threadMXBean.getThreadCount() +
                            ", peakT: " + threadMXBean.getPeakThreadCount() +
                            ", totalStartedT: " + threadMXBean.getTotalStartedThreadCount() +
                            ", WORKERS_AMOUNT: " + i);
                }
                final WorkerManagerMeta workerManager = workerManagerMetas.get(i);
                workerManager.send(new Message_SWM_Setup(nodesToAddLight, nodesToRemoveLight, trafficHashVal));
            }
        }

        serverTimings.stopSendingSetup(System.out);

        serverLogger.info(" [Timer] End - stop sending setup to workers timer.");

        System.out.println("Sent simulation configuration to all worker managers.");


    }

    public void setupNewSimWithWorkers(){
        System.out.println("SERVER START SETUP");
        int trafficHashVal = roadNetwork.generateHash();
        // Reset temporary variables
        simulationWallTime = 0;
        step = 0;
        totalNumWwCommChannels = 0;
        numTrajectoriesReceived = 0;
        numVehiclesCreatedDuringSetup = 0;
        numVehiclesNeededAtStart = 0;
        receivedTrafficReportCache.clear();
        aggregatedVehicleCountInOneSimulation = 0.0;
        aggregatedVehicleTravelSpeedInOneSimulation = 0.0;

        // Reset worker status
        for (final WorkerMeta worker : workerMetas) {
            worker.setState(WorkerState.NEW);
        }

        serverLogger.info(" [Timer] Start partitioning grid cell timer.");
        serverTimings.startPartitionGridCells();

        System.out.println("SERVER PARTITION CELLS");
        if (Settings.isNewEnvironment) {
            WorkloadBalancer.partitionGridCells(workerMetas, roadNetwork, totalLanesLength);
        }
        serverLogger.info(" [Timer] End partitioning grid cell timer.");
        serverTimings.stopPartitionGridCells(System.out);


        // Determine the number of internal vehicles at all workers
        WorkloadBalancer.assignNumInternalVehiclesToWorkers(workerMetas, roadNetwork);

        // Assign vehicle routes from external file to workers
        final RouteLoader routeLoader = new RouteLoader(this, workerMetas);
        routeLoader.loadRoutes();

        // Get number of vehicles needed
        numVehiclesNeededAtStart = routeLoader.vehicles.size() + Settings.numGlobalRandomBackgroundPrivateVehicles
                + Settings.numGlobalBackgroundRandomTrams + Settings.numGlobalBackgroundRandomBuses;

//		initWallTime = getInitializationTime();
//		setupStartTimeStamp = System.nanoTime();

        serverLogger.info(" [Timer] End - Stop Initialization timer and whole processs timer.");
        serverLogger.info(" [Timer] Start setup timer.");
        serverTimings.stopFirstAndStartSecStage();

        serverTimings.startSendingSetup();
        if (connectedOnlyWm){
            ArrayList<Message_SW_Setup> messages = new ArrayList<>();
            String workerManagerName = workerManagerMetas.get(0).name;
            ArrayList<WorkerMeta> workerMetaArrayListInWm = workerMetasInWorkerManager.get(workerManagerName);
            WorkerMeta firstWorker = workerMetaArrayListInWm.get(0);
            Message_SW_Setup template = new Message_SW_Setup(firstWorker, nodesToAddLight, nodesToRemoveLight);
            messages.add(template);
            for(int i =1; i < workerMetaArrayListInWm.size(); i++){
                WorkerMeta worker = workerMetaArrayListInWm.get(0);
                messages.add(new Message_SW_Setup(template, worker, false));
            }
            Message_SWM_WorkerSetup message = new Message_SWM_WorkerSetup(workerMetas, step, workerManagerName, messages);
            int templateHash = message.generateHash();
            System.out.println("SEND SETUP OF ALL WORKER MANAGES ");
            asyncThreadedMsgSenderToWorkerManager.sendOutWorkersSetupToAllWorkerManagers(message, SendMessageType.Message_SWM_Worker_Setup, templateHash, workerMetasInWorkerManager);


        } else {
            if (Settings.parallelMessageSending) {
                serverLogger.info(" [Timer] Start sending setup to workers timer - Parallel.");
                final WorkerMeta worker = workerMetas.get(0);
                Message_SW_Setup template = new Message_SW_Setup(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal);
                int templateHash = template.generateHash();

                asyncThreadedMsgSenderToWorker.sendOutToAllWorkers(template, SendMessageType.Message_SW_Setup, templateHash);
//			asyncThreadedMsgSender.sendOutToAllWorkers(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal, SendMessageType.Message_SW_Setup);
            }
            else{
                serverLogger.info(" [Timer] Start sending setup to workers timer - Sequential.");
                for (int i = 0; i < workerMetas.size(); i++) {
                    if (i % 20 == 0) {
                        System.out.println("[Server][SENDING_CONFIG] HeapTotal: " + runtime.totalMemory() + ", HeapFree: " + runtime.freeMemory() +
                                ", activeT: " + threadMXBean.getThreadCount() +
                                ", peakT: " + threadMXBean.getPeakThreadCount() +
                                ", totalStartedT: " + threadMXBean.getTotalStartedThreadCount() +
                                ", WORKERS_AMOUNT: " + i);
                    }
                    final WorkerMeta worker = workerMetas.get(i);
                    worker.send(new Message_SW_Setup(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal));
                }
            }
        }


        serverTimings.stopSendingSetup(System.out);

        serverLogger.info(" [Timer] End - stop sending setup to workers timer.");


        // Initialize output
        fileOutput.init();

        Settings.isNewEnvironment = false;

        System.out.println("Sent simulation configuration to all workers.");

    }

	public void setupNewSim() {
		// Reset temporary variables
		simulationWallTime = 0;
		step = 0;
		totalNumWwCommChannels = 0;
		numTrajectoriesReceived = 0;
		numVehiclesCreatedDuringSetup = 0;
		numVehiclesNeededAtStart = 0;
		receivedTrafficReportCache.clear();
		aggregatedVehicleCountInOneSimulation = 0.0;
		aggregatedVehicleTravelSpeedInOneSimulation = 0.0;

		// Reset worker status
		for (final WorkerMeta worker : workerMetas) {
			worker.setState(WorkerState.NEW);
		}

		serverLogger.info(" [Timer] Start partitioning grid cell timer.");
		serverTimings.startPartitionGridCells();
		// In a new environment (map), determine the work areas for all workers
		if (Settings.isNewEnvironment) {
			roadNetwork.buildGrid();
			WorkloadBalancer.partitionGridCells(workerMetas, roadNetwork, totalLanesLength);
		}
		serverLogger.info(" [Timer] End partitioning grid cell timer.");
		serverTimings.stopPartitionGridCells(System.out);


		// Determine the number of internal vehicles at all workers
		WorkloadBalancer.assignNumInternalVehiclesToWorkers(workerMetas, roadNetwork);

		// Assign vehicle routes from external file to workers
		final RouteLoader routeLoader = new RouteLoader(this, workerMetas);
		routeLoader.loadRoutes();

		// Get number of vehicles needed
		numVehiclesNeededAtStart = routeLoader.vehicles.size() + Settings.numGlobalRandomBackgroundPrivateVehicles
				+ Settings.numGlobalBackgroundRandomTrams + Settings.numGlobalBackgroundRandomBuses;

//		initWallTime = getInitializationTime();
//		setupStartTimeStamp = System.nanoTime();

		serverLogger.info(" [Timer] End - Stop Initialization timer and whole processs timer.");
		serverLogger.info(" [Timer] Start setup timer.");
		serverTimings.stopFirstAndStartSecStage();


		final GridCell[][] grid = roadNetwork.grid;

		for (int i = 0; i < Settings.numGridRows; i++) {
			for (int j = 0; j < Settings.numGridCols; j++) {
				totalLanesLength += grid[i][j].laneLength;
			}
		}

		System.out.println("Map road Graph length: " + Settings.roadGraph.length() +
				", Nodes: " + roadNetwork.nodes.size() +
				", Edges: " + roadNetwork.edges.size() +
				", Lanes: " + roadNetwork.lanes.size() +
				", LaneLengthOfWholeMap: " + totalLanesLength +
				", minLat: " + roadNetwork.minLat + ", minLon: " + roadNetwork.minLon +
				", maxLat: " + roadNetwork.maxLat + ", maxLon: " + roadNetwork.maxLon +
				", mapWidth: " + roadNetwork.mapWidth + ", mapHeight: " + roadNetwork.mapHeight);

//		serverTimings.startSendingSetup();
//		// Send simulation configuration to workers
//		for (final WorkerMeta worker : workerMetas) {
//			worker.send(new Message_SW_Setup(workerMetas, worker, roadNetwork.edges, step, nodesToAddLight,
//					nodesToRemoveLight));
//		}
//		serverTimings.stopSendingSetup(System.out);


//		serverTimings.startSendingSetupConfirmation();
//		for (final WorkerMeta worker : workerMetas) {
//			worker.send(new Message_SW_SetupSentAll());
//		}
//		serverTimings.stopSendingSetupConfirmation(System.out);




		int trafficHashVal = roadNetwork.generateHash();
		System.out.println("Server hash of map: " + trafficHashVal);
        //HERE START TO SEND TO WMS!!!
		serverTimings.startSendingSetup();
		if (Settings.parallelMessageSending) {
			serverLogger.info(" [Timer] Start sending setup to workers timer - Parallel.");
			final WorkerMeta worker = workerMetas.get(0);
			Message_SW_Setup template = new Message_SW_Setup(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal);
			int templateHash = template.generateHash();

			asyncThreadedMsgSenderToWorker.sendOutToAllWorkers(template, SendMessageType.Message_SW_Setup, templateHash);
//			asyncThreadedMsgSender.sendOutToAllWorkers(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal, SendMessageType.Message_SW_Setup);
		}
		else{
			serverLogger.info(" [Timer] Start sending setup to workers timer - Sequential.");
			for (int i = 0; i < workerMetas.size(); i++) {
				if (i % 20 == 0) {
					System.out.println("[Server][SENDING_CONFIG] HeapTotal: " + runtime.totalMemory() + ", HeapFree: " + runtime.freeMemory() +
							", activeT: " + threadMXBean.getThreadCount() +
							", peakT: " + threadMXBean.getPeakThreadCount() +
							", totalStartedT: " + threadMXBean.getTotalStartedThreadCount() +
							", WORKERS_AMOUNT: " + i);
				}
				final WorkerMeta worker = workerMetas.get(i);
				worker.send(new Message_SW_Setup(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal));
			}
		}

		serverTimings.stopSendingSetup(System.out);

		serverLogger.info(" [Timer] End - stop sending setup to workers timer.");


		// Initialize output
		fileOutput.init();

		Settings.isNewEnvironment = false;

		System.out.println("Sent simulation configuration to all workers.");

	}

	public void askWorkersChangeLaneBlock(int laneIndex, boolean isBlocked) {
		for (final WorkerMeta worker : workerMetas) {
			worker.send(new Message_SW_BlockLane(laneIndex, isBlocked));
		}
	}

	void startSimulation() {
		if (step < Settings.maxNumSteps) {
			System.out.println("All workers are ready to do simulation.");
			isSimulating = true;
			if (Settings.isVisualize) {
				gui.startSimulation();
			}
			if (Settings.isServerBased) {
				System.out.println("Starting server-based simulation...");
				askWorkersShareTrafficDataWithFellowWorkers();
			} else {
				System.out.println("Starting server-less simulation...");
				askWorkersProceedWithoutServer();
			}
		}

	}

	void startSimulationFromLoadedScript() {
        /*if (scriptLoader.retrieveOneSimulationSetup()) {
            changeMap();
        }*/
		setupNewSimWithWorkers();
		//HERE important change!!
		//setupNewSim();
	}

	public void stopSim() {
//		fileDumpingStartTimeStamp = System.nanoTime();
		isSimulating = false;

		serverLogger.info("Dumping collected metrics into files... Q:" + messageProcessor.getQueueSize() +
				", cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad());

		serverLogger.info(" [Timer] Start file dumping timer.");
		serverTimings.startFileDumping();

		numInternalNonPubVehiclesAtAllWorkers = 0;
		numInternalTramsAtAllWorkers = 0;
		numInternalBusesAtAllWorkers = 0;
		nodesToAddLight.clear();
		nodesToRemoveLight.clear();
		processCachedReceivedTrafficReports();
		fileOutput.outputSimLog(step, simulationWallTime, totalNumWwCommChannels, aggregatedVehicleCountInOneSimulation,
				aggregatedVehicleTravelSpeedInOneSimulation);
		aggregatedVehicleCountInOneSimulation = 0.0;
		aggregatedVehicleTravelSpeedInOneSimulation = 0.0;

		fileOutput.outputRoutes(routesForOutput);
		routesForOutput.clear();
		fileOutput.outputTrajectories(trajectoriesForOutput);   // TODO UNCOMMENT
		trajectoriesForOutput.clear();
		fileOutput.outputTravelTime(travelTimesForOutput);
		travelTimesForOutput.clear();
		fileOutput.close();
		serverLogger.info(" [Timer] End - stop file dumping timer.");
		serverTimings.stopFileDumping();
		serverLogger.info("DONE - Dumping into files.");

		// Ask workers stop in server-less mode. Note that workers may already stopped
		// before receiving the message.


		/*serverLogger.info("Sending SW_SERVERLESS_STOP to workers...");
		serverTimings.startSendingStop();
		if (!Settings.isServerBased) {
			if (Settings.parallelMessageSending) {
				asyncThreadedMsgSenderToWorkerManager.sendOutToAllWorkersManagers(SendMessageType.Message_SW_Serverless_Stop, step);
			} else {
				final Message_SW_Serverless_Stop message = new Message_SW_Serverless_Stop(step);
				for (final WorkerManagerMeta workerManager : workerManagerMetas) {
					workerManager.send(message);
					workerManager.setState(WorkerState.NEW);
				}
			}
		}
		serverTimings.stopSendingStop(System.out);
		serverLogger.info("Sent stop msg to all workers!");

		serverLogger.info(" [Timer] End - stop whole process timer.");
		serverTimings.stopWholeProcess();*/

		serverTimings.logResults(serverLogger);
        System.out.println("TOTAL CONNECTIONS BETWEEN WORKERS = " + totalNumWwCommChannels);
		System.out.println("Simulation stopped.\n");

		System.out.println("Received reports = " + trafficReportCounter);
		serverLogger.info("Received reports = " + trafficReportCounter);

		System.out.println("Server prev simulation time = " + simulationWallTime + "\n");
		serverLogger.info("[Deprecated] Server prev simulation time = " + simulationWallTime);
		serverTimings.printResults(System.out);

		serverLogger.info(" [Timer] All results printed and restarted initialization and whole process timer.");
		serverTimings.startFirstStage();

//		close();
	}

	public void closeSim(){
		messageProcessor.poisonAllProcessors();
//		initStartTimeStamp = System.nanoTime();

		if ((Settings.isVisualize)) {
			if (workerMetas.size() == Settings.numWorkers) {
				gui.getReadyToSetup();
			}
		} else {
			if (scriptLoader.isEmpty()) {
				acceptConsoleCommandAtSimEnd();
			} else {
				System.out.println("Loading configuration of new simulation...");
				startSimulationFromLoadedScript();
			}
		}
	}

	/**
	 * Updates wall time spent on simulation.
	 */
	synchronized void updateSimulationTime() {
		simulationWallTime += (double) (System.nanoTime() - timeStamp) / 1000000000;
	}

	synchronized void updateWorkerState(final String workerName, final WorkerState state) {
        workerMetasByName.get(workerName).setState(state);
	}
}


