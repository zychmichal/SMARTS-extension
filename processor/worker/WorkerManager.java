package processor.worker;

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
import traffic.light.LightUtil;
import traffic.road.Edge;
import traffic.road.RoadNetwork;
import traffic.road.RoadUtil;
import traffic.routing.RouteUtil;

import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;

/**
 * WorkerManager is class to keep every common worker data in one place and let to run all workers on 1 JVM.
 */
public class WorkerManager implements MessageHandler {

    public CreateVehiclesService createVehiclesService;
    public static boolean useServiceToCreateVehicles = false;
    private ExecutorService pool;
    public ExecutorService poolService;
    public static int nrWorkers;
    public int toCreateFromFile=0;
    public int doneCreateWorker=0;
    public int trafficReportIndex=0;
    public int finishSimulation=0;
    public int finishTrajectory=0;
    ArrayList<Message_WS_TrafficReport> trafficReports = new ArrayList<>();
    ArrayList<Message_WS_Serverless_Complete> serverlessCompletes = new ArrayList<>();
    public int simulationCompleteIndex=0;
    private File[] trajectoryFiles;
    private int trajectoryIndex=0;
    private HashMap<String, HashMap<Integer, Edge>> workersWithStartEdges = new HashMap<>();
    public static String routesDir = "";
    public static boolean readRoutes = false;
    public static String address = "localhost";
    public static boolean printStream = false;
    public String name;

    //To know if every worker are ready to reset all Trafics.
    public int toClearNetworkCounter=0;
    private HashMap<String,Worker> workers = new HashMap<>();

    public static int percentToCreate = 10;
    public RoadNetwork roadNetwork;
    private int listeningPort;
    private MessageProcessor messageProcessor;
    private IncomingConnectionBuilder connectionBuilder;
    private MessageSender senderForServer;
    private static Logger logger;
    private boolean readyToCreateWorker = false;
    long printRuntimeStatusTimeStamp = System.nanoTime();

    Runtime runtime = Runtime.getRuntime();
    java.lang.management.ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public static void main(String[] args) throws FileNotFoundException {
        if (processCommandLineArguments(args)){
            try{
                new WorkerManager().run();
            } catch (Exception e){
               e.printStackTrace();
               System.exit(1000);
            }

        }
    }

    private static boolean processCommandLineArguments(final String[] args) {
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-n":
                        nrWorkers = Integer.parseInt(args[i + 1]);
                        break;
                    case "-percent":
                        percentToCreate = Integer.parseInt(args[i + 1]);
                        break;
                    case "-server_address":
                        Settings.serverAddress = args[i + 1];
                        break;
                    case "-print":
                        printStream = true;
                        break;
                    case "-read_routes":
                        readRoutes = true;
                        routesDir = args[i + 1];
                        break;
                    case "-create-service":
                        useServiceToCreateVehicles = true;
                }
            }
        }catch (final Exception e) {
            return false;
        }
        return true;
    }

    public synchronized void saveTrafficReportIndex(Message_WS_TrafficReport messageWsTrafficReport){
        trafficReportIndex++;
        trafficReports.add(messageWsTrafficReport);
        if(trafficReportIndex==nrWorkers){
            trafficReportIndex=0;
            Message_WM_TrafficReport message = new Message_WM_TrafficReport(name, trafficReports);
            senderForServer.send(message);
            trafficReports.clear();
        }

    }

    public synchronized void incrementFinishSimulation(Message_WS_Serverless_Complete message){
        finishSimulation++;
        if(WorkerManager.printStream) System.out.println("FINISH SIMULATION: " + finishSimulation);
        serverlessCompletes.add(message);
        if(finishSimulation==nrWorkers){
            finishSimulation=0;
            if(WorkerManager.printStream) System.out.println("SEND SERVERLESS");
            Message_WM_Serverless_Complete msg = new Message_WM_Serverless_Complete(name, serverlessCompletes);
            senderForServer.send(msg);
            serverlessCompletes.clear();
        }
    }

    public synchronized void incrementFinishTrajectory(){
        finishTrajectory++;
        if(finishTrajectory==nrWorkers){
            senderForServer.send(new Message_WS_DumpingDone());
        }
    }

    public void run() throws FileNotFoundException {
        //CREATE PULL FOR ALL WORKERS
        pool = Executors.newFixedThreadPool(WorkerManager.nrWorkers);
        poolService = Executors.newFixedThreadPool(2);
        //CONNECT TO SERVER
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

        address = SysUtil.getMyIpV4Addres();

        name = "WorkerManager:" + address + ":" + listeningPort;
        if(WorkerManager.printStream) {
            String fileName = name.replace(".","_").replace(":","_");
            String outFilename = "./out_" + fileName + ".txt";
            System.out.println(outFilename);
            PrintStream fileOut = new PrintStream(outFilename);
            System.setOut(fileOut);
        }


        logger = LogManager.getLogger("WorkerManagerLogger");
        // Prepare message processing units
        messageProcessor = new MessageProcessor(name, 24, this);
        messageProcessor.runAllProccessors();

        connectionBuilder = new IncomingConnectionBuilder(messageProcessor, listeningPort, this, ss);
        connectionBuilder.setName("[" + name + "][ConnectionBuilder]");
        connectionBuilder.start();

//        System.out.printf("SERVER ADDRESS: %s, SERVER PORT: %d\n", Settings.serverAddress, Settings.serverListeningPortForWorkers);
        System.out.printf("WM ADDRESS: %s, WM PORT: %d\n", address, listeningPort);
        senderForServer = new MessageSender(Settings.serverAddress, Settings.serverListeningPortForWorkers, name);

        senderForServer.send(new Message_WM_Join(name, address, listeningPort));

        if(WorkerManager.printStream) System.out.println("WorkerManager (" + name + ") WM CONNECTED!");
    }

    public synchronized void incrementDoneCreateWorker(Worker worker){
        workers.put(worker.name, worker);
        doneCreateWorker++;
        if(doneCreateWorker==nrWorkers){
            doneCreateWorker=0;
            ArrayList<SerializableWorkerInitData> initWorkersData = workers.values().stream().map(SerializableWorkerInitData::new).collect(Collectors.toCollection(ArrayList::new));
            senderForServer.send(new Message_WM_FinishCreateWorkers(this, initWorkersData));
        }
    }

    public synchronized void incrementDoneSetupWorker(){
        doneCreateWorker++;
        if(doneCreateWorker==nrWorkers){
            System.out.println("DONE SETUP WORKER MANAGER " + name);
            doneCreateWorker=0;
            ArrayList<Message_WS_SetupDone> setupDones = workers.values().stream().map(w -> new Message_WS_SetupDone(w.name, w.connectedFellows.size())).collect(Collectors.toCollection(ArrayList::new));
            senderForServer.send(new Message_WM_FinishSetupWorkers(this.name, setupDones));
        }
    }

    public synchronized void incrementToCreateFromFile(Worker worker, boolean finish){
        toCreateFromFile++;
        if(finish){
            workersWithStartEdges.remove(worker.name);
        }
        if(toCreateFromFile == nrWorkers){
            if(workersWithStartEdges.isEmpty()){
                System.out.println("ALL WORKERS HAVE VEHICLES -> DONT NEED TO READ NEXT FILES");
                for(var entry: workers.entrySet()){
                    System.out.println("FINISH READING VEHICLES: " + name);
                    pool.execute(() -> entry.getValue().finishSetup());
                }
            } else {
                readNextFileFromDirectory();
            }

        }
    }

    public synchronized void incrementStartCreatingVehiclesFromFiles(){
        toCreateFromFile++;
        if(toCreateFromFile >= nrWorkers){
            readWholeRouteDirectory();
        }
    }

    public void readNextFileFromDirectory(){
        readRoutesFromFile(trajectoryFiles[trajectoryIndex]);
    }

    public void readWholeRouteDirectory(){
        final File routeFolder = new File(routesDir);
        workers.forEach( (name, worker) -> {
            HashMap<Integer, Edge> edges = new HashMap<>();
            worker.trafficNetwork.internalNonPublicVehicleStartEdges.forEach(e -> edges.put(e.index,e));
            workersWithStartEdges.put(name, edges);
        });

        trajectoryFiles = routeFolder.listFiles();

        if(trajectoryFiles!=null){
            readRoutesFromFile(trajectoryFiles[trajectoryIndex]);
        } else {
            System.out.println("EMPTY DIRECTORY!!!!");
            System.exit(123456);
        }


    }

    public void readRoutesFromFile(File trajectoryFile){
        System.out.println("START READING FROM FILE: " + trajectoryFile.getName());
        var start = System.nanoTime();
        HashMap<String, ArrayList<String []>> workersWithRoutes = new HashMap<>();
        workers.keySet().forEach(k -> workersWithRoutes.put(k, new ArrayList<>()));

        BufferedReader routeFile = null;
        try {
            routeFile = new BufferedReader(new FileReader(trajectoryFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(13579);
        }


        routeFile.lines().forEach(l -> {
            String [] splitRoute = l.split(",");
            int firstIndex = Integer.parseInt(splitRoute[1]);
            String workerName = findIfIsInWorker(firstIndex, workersWithStartEdges);
            if(workerName==null) return;

            var line = workersWithRoutes.get(workerName);
            line.add(splitRoute);
        });

        try {
            routeFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(13579);
        }

        System.out.println("PROCESSING FILE: " + trajectoryFile.getName() + " TOTAL TIME: " + (System.nanoTime()-start)/1e9);
        toCreateFromFile=0;
        trajectoryIndex++;

        for(var entry: workers.entrySet() ){
            var routes = workersWithRoutes.get(entry.getKey());
            Worker worker = entry.getValue();

            if(trajectoryIndex >= trajectoryFiles.length){
                //execute poll here
                System.out.println("FINISH READING VEHICLES: " + name);
                pool.execute(() -> worker.finishCreatingVehicles(routes));
            } else {
                pool.execute(() -> worker.createVehiclesFromBatch(routes));
                // here calling other method to only create vehhicles /also check if is over/
            }

        }


    }

    private String findIfIsInWorker(int index,  HashMap<String, HashMap<Integer, Edge>> workersWithStartEdges){
        for(Map.Entry<String,HashMap<Integer,Edge>> entry: workersWithStartEdges.entrySet()){
            Edge edge = entry.getValue().get(index);
            if(edge!=null) return entry.getKey();
        }
        return null;
    }


    public synchronized void incrementToClearNetworkCounter(){
        toClearNetworkCounter++;
        if(toClearNetworkCounter == nrWorkers){
            resetRoadNetwork();
        }
    }

    public void resetRoadNetwork(){

    }

    @Override
    public void processReceivedMsg(Object message) {
        if (message instanceof Message_SWM_Setup) {
            final Message_SWM_Setup received = (Message_SWM_Setup) message;
            Settings.numWorkers = received.numWorkers;
            Settings.maxNumSteps = received.maxNumSteps;
            Settings.numStepsPerSecond = received.numStepsPerSecond;
            Settings.partitionType = received.partitionType;
            Settings.trafficReportStepGapInServerlessMode = received.workerToServerReportStepGapInServerlessMode;
            Settings.periodOfTrafficWaitForTramAtStop = received.periodOfTrafficWaitForTramAtStop;
            //Settings.driverProfileDistribution = setDriverProfileDistribution(received.driverProfileDistribution);
            Settings.lookAheadDistance = received.lookAheadDistance;
            Settings.trafficLightTiming = LightUtil.getLightTypeFromString(received.trafficLightTiming);
            Settings.isVisualize = received.isVisualize;
            Settings.isServerBased = received.isServerBased;
            Settings.routingAlgorithm = RouteUtil.getRoutingAlgorithmFromString(received.routingAlgorithm);
            Settings.isAllowPriorityVehicleUseTramTrack = received.isAllowPriorityVehicleUseTramTrack;
            Settings.outputRouteScope = DataOutputScope.valueOf(received.outputRouteScope);
            Settings.outputTrajectoryScope = DataOutputScope.valueOf(received.outputTrajectoryScope);
            Settings.outputTravelTimeScope = DataOutputScope.valueOf(received.outputTravelTimeScope);
            Settings.isOutputSimulationLog = received.isOutputSimulationLog;
            /*Settings.listRouteSourceWindowForInternalVehicle = setRouteSourceDestinationWindow(
                    received.listRouteSourceWindowForInternalVehicle);
            Settings.listRouteDestinationWindowForInternalVehicle = setRouteSourceDestinationWindow(
                    received.listRouteDestinationWindowForInternalVehicle);
            Settings.listRouteSourceDestinationWindowForInternalVehicle = setRouteSourceDestinationWindow(
                    received.listRouteSourceDestinationWindowForInternalVehicle);*/
            Settings.isAllowReroute = received.isAllowReroute;
            Settings.isAllowTramRule = received.isAllowTramRule;
            Settings.isDriveOnLeft = received.isDriveOnLeft;
            Settings.oneSimulation = received.oneSimulation;
            Settings.numGlobalRandomBackgroundPrivateVehicles = received.numGlobalRandomBackgroundPrivateVehicles;

            boolean loadityourself = false;

            if (received.roadGraph.equals("builtin")) {
                Settings.roadGraph = RoadUtil.importBuiltinRoadGraphFile();
            } else if (received.roadGraph.equals("loadityourself")){
                loadityourself = buildOSMmap(received);
            } else {
                Settings.roadGraph = received.roadGraph;
            }

            roadNetwork = new RoadNetwork();

            // log results of processing map
            if (loadityourself) {
                final int roadTrafficNetworkHash = roadNetwork.generateHash();
                if (received.mapHashValue != roadTrafficNetworkHash) {
                    System.out.println("[ERROR] MAP HASHES: server: " + received.mapHashValue + ", WORKER_" + name + ": " + roadTrafficNetworkHash);
                    System.err.println("[ERROR] MAP HASHES: server: " + received.mapHashValue + ", WORKER_" + name + ": " + roadTrafficNetworkHash);
                    logger.error("[ERROR] MAP HASHES: server: " + received.mapHashValue + ", WORKER_" + name + ": " + roadTrafficNetworkHash);
                }
                else{
                    logger.info("[OSM][" + name + "] Road network graph built based on OSM file: "
                            + Settings.inputOpenStreetMapFile + ", hash: " + roadTrafficNetworkHash +
                            ", processors=" + runtime.availableProcessors() +
                            ", activeT: " + threadMXBean.getThreadCount() +
                            ", peakT: " + threadMXBean.getPeakThreadCount() +
                            ", totalStartedT: " + threadMXBean.getTotalStartedThreadCount());
                }
            }

            senderForServer.send(new Message_WM_SetupDone(name));

//            System.out.println("WorkerManager (" + name + ") SETUP DONE!");
            readyToCreateWorker = true;
        }
        else if (message instanceof Message_SWM_CreateWorkers){
            if (readyToCreateWorker){
                create_all_workers();
            }
        } else if (message instanceof Message_SWM_WorkerSetup){
            final Message_SWM_WorkerSetup received = (Message_SWM_WorkerSetup) message;
            for(Message_SW_Setup workerSetupMessage: received.messages){
                System.out.println(workerSetupMessage);
                workerSetupMessage.metadataWorkers = received.metadataWorkers;
                workerSetupMessage.isNewEnvironment = received.isNewEnvironment;
                workerSetupMessage.partitionType = received.partitionType;
                pool.execute(() -> workers.get(workerSetupMessage.workerName).processReceivedMsg(workerSetupMessage));
            }
        } else if (message instanceof Message_SW_Serverless_Start){
            workers.values().forEach(w -> pool.execute(() -> w.processReceivedMsg(message)));
        } else if (message instanceof Message_SW_Serverless_Stop){
            workers.values().forEach(w -> pool.execute(() -> w.processReceivedMsg(message)));
        } else if (message instanceof Message_SW_KillWorker){
            workers.values().forEach(w -> pool.execute(() -> w.processReceivedMsg(message)));
        }
    }

    public void logAndPrintMachineState(int step) {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Object attribute2 = null;
        Object attribute = null;
        try {
            attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
            attribute2 = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
        } catch (MBeanException e) {
            e.printStackTrace();
            logger.error("Server MBeanException while accessing MBeanException bean", e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Server Exception while reading MBeanException bean", e);
        }

        MemoryUsage MU = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        String strHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
                "used=" + (MU.getUsed() >> 10) + "K| " +
                "committed=" + (MU.getCommitted() >> 10) + "K| " +
                "max=" + (MU.getMax() >> 10) + "K}";
        String stringHeapMU = (MU.getInit() >> 10) +
                ", " + (MU.getUsed() >> 10)  +
                ", " + (MU.getCommitted() >> 10) +
                ", " + (MU.getMax() >> 10) ;

        MU = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        String strNonHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
                "used=" + (MU.getUsed() >> 10) + "K| " +
                "committed=" + (MU.getCommitted() >> 10) + "K| " +
                "max=" + (MU.getMax() >> 10) + "K}";
        String stringNonHeapMU = (MU.getInit() >> 10) +
                ", " + (MU.getUsed() >> 10)  +
                ", " + (MU.getCommitted() >> 10) +
                ", " + (MU.getMax() >> 10) ;
        logger.info("[LOG_MACHINE_STATE] cpus=" + runtime.availableProcessors() +
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
        System.out.println("[LMS], " + step + ", " + name + ", "  + runtime.availableProcessors() +
                ", " + threadMXBean.getThreadCount() +
                ", " + threadMXBean.getPeakThreadCount() +
                ", " + threadMXBean.getTotalStartedThreadCount() +
                ", " + getOperatingSystemMXBean().getAvailableProcessors() +
                ", " + getOperatingSystemMXBean().getSystemLoadAverage() +
                ", " + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getProcessCpuLoad() +
                ", " + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad() +
                ", " + stringHeapMU +
                ", " + stringNonHeapMU +
                ", " + ((double)runtime.maxMemory()/(1024*1024)) +
                ", " + ((double)runtime.totalMemory()/(1024*1024)) +
                ", " + ((double)runtime.freeMemory()/(1024*1024))+
                ", " + Long.parseLong(attribute.toString()) / 1024 /1024 +"MB" +
                ", " + Long.parseLong(attribute2.toString()) / 1024 /1024 +"MB");
    }

    public void printBatchWholeSimulationTime(int step){
        System.out.println("TIME OF 50 iteration, step: " + step + ", time: " + ((System.nanoTime() - printRuntimeStatusTimeStamp)/1.0E9));
        printRuntimeStatusTimeStamp = System.nanoTime();
    }


    private void create_all_workers(){
        //CREATE VEHICLE SERVICE
        if(useServiceToCreateVehicles){
            createVehiclesService = new CreateVehiclesService(this, roadNetwork);
            if(WorkerManager.printStream) System.out.println("VEHICLE SERVICE CREATED: " + createVehiclesService);
            Worker firstWorker = new Worker(roadNetwork, this,true);
            pool.execute(firstWorker);
        } else{
            Worker firstWorker = new Worker(roadNetwork, this);
            pool.execute(firstWorker);
        }
        for(int i=0; i < nrWorkers -1 ; i++){
            Worker worker = new Worker(roadNetwork, this);
            pool.execute(worker);
        }
    }

    private boolean buildOSMmap(final Message_SWM_Setup received) {
        System.out.println("WORKER MANAGER START BUILD OSM MAP");
        var start = System.nanoTime();
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
        System.out.println("WORKER MANAGER STOP BUILD OSM MAP TIME: " + (System.nanoTime()-start)/1e9);
//				osm.dumpMapToFileDebugMethod("Worker_" + name.replace(':','-') + "_Roads.txt");
        return true;
    }
}
