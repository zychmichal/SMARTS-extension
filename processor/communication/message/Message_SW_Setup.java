package processor.communication.message;

import java.util.ArrayList;

import common.Settings;
import processor.server.WorkerMeta;
import traffic.road.Edge;
import traffic.road.Lane;
import traffic.road.Node;

/**
 * Server-to-worker message containing the simulation configuration. Worker will
 * set up simulation environment upon receiving this message.
 */
public class Message_SW_Setup {
    public String workerName;
	public boolean isNewEnvironment;
	public int startStep;
	public int numRandomPrivateVehicles;
	public int numRandomTrams;
	public int numRandomBuses;
	public ArrayList<SerializableExternalVehicle> externalRoutes = new ArrayList<>();
	/**
	 * Metadata of all workers
	 */
	public ArrayList<SerializableWorkerMetadata> metadataWorkers = new ArrayList<>();
    public String partitionType;
    /**
     * Index of nodes where traffic light is added by user
     */
    public ArrayList<SerializableInt> indexNodesToAddLight = new ArrayList<>();
    /**
     * Index of nodes where traffic light is removed by user
     */
    public ArrayList<SerializableInt> indexNodesToRemoveLight = new ArrayList<>();

	public Message_SW_Setup(Message_SW_Setup template, final WorkerMeta workerToReceiveMessage, final boolean onlyWorkers) {
		if(onlyWorkers){
            this.isNewEnvironment = template.isNewEnvironment;
            this.startStep = template.startStep;
            this.partitionType = template.partitionType;
            this.metadataWorkers = template.metadataWorkers;
        }

		this.numRandomPrivateVehicles = workerToReceiveMessage.numRandomPrivateVehicles;
		this.numRandomTrams = workerToReceiveMessage.numRandomTrams;
		this.numRandomBuses = workerToReceiveMessage.numRandomBuses;
		this.externalRoutes = workerToReceiveMessage.externalRoutes;
        this.indexNodesToAddLight = template.indexNodesToAddLight;
        this.indexNodesToRemoveLight = template.indexNodesToRemoveLight;
        this.workerName = workerToReceiveMessage.name;
	}

	public Message_SW_Setup() {

	}

    public Message_SW_Setup(final WorkerMeta workerToReceiveMessage, final ArrayList<Node> nodesToAddLight,
                            final ArrayList<Node> nodesToRemoveLight) {
        workerName = workerToReceiveMessage.name;
//        isNewEnvironment = Settings.isNewEnvironment;
//        startStep = step;
//        //to save some memory -> move metadata workers to Message_SWM_WorkerSetup
//        metadataWorkers = appendMetadataOfWorkers(workers);
        numRandomPrivateVehicles = workerToReceiveMessage.numRandomPrivateVehicles;
        numRandomTrams = workerToReceiveMessage.numRandomTrams;
        numRandomBuses = workerToReceiveMessage.numRandomBuses;
        externalRoutes = workerToReceiveMessage.externalRoutes;
        indexNodesToAddLight = getLightNodeIndex(nodesToAddLight);
        indexNodesToRemoveLight = getLightNodeIndex(nodesToRemoveLight);
//        partitionType = Settings.partitionType;

    }

	public Message_SW_Setup(final ArrayList<WorkerMeta> workers, final WorkerMeta workerToReceiveMessage, final int step, final ArrayList<Node> nodesToAddLight,
							final ArrayList<Node> nodesToRemoveLight, final int mapValue) {
		workerName = workerToReceiveMessage.name;
        isNewEnvironment = Settings.isNewEnvironment;
		startStep = step;
        //to save some memory -> move metadata workers to Message_SWM_WorkerSetup
		metadataWorkers = appendMetadataOfWorkers(workers);
		numRandomPrivateVehicles = workerToReceiveMessage.numRandomPrivateVehicles;
		numRandomTrams = workerToReceiveMessage.numRandomTrams;
		numRandomBuses = workerToReceiveMessage.numRandomBuses;
		externalRoutes = workerToReceiveMessage.externalRoutes;
        indexNodesToAddLight = getLightNodeIndex(nodesToAddLight);
        indexNodesToRemoveLight = getLightNodeIndex(nodesToRemoveLight);
        partitionType = Settings.partitionType;

    }

    public Message_SW_Setup(final WorkerMeta workerToReceiveMessage, final int step, final ArrayList<Node> nodesToAddLight,
                            final ArrayList<Node> nodesToRemoveLight, final int trafficHashVal) {
        workerName = workerToReceiveMessage.name;
        isNewEnvironment = Settings.isNewEnvironment;
        startStep = step;
        //to save some memory -> move metadata workers to Message_SWM_WorkerSetup
        numRandomPrivateVehicles = workerToReceiveMessage.numRandomPrivateVehicles;
        numRandomTrams = workerToReceiveMessage.numRandomTrams;
        numRandomBuses = workerToReceiveMessage.numRandomBuses;
        externalRoutes = workerToReceiveMessage.externalRoutes;
        indexNodesToAddLight = getLightNodeIndex(nodesToAddLight);
        indexNodesToRemoveLight = getLightNodeIndex(nodesToRemoveLight);
        partitionType = Settings.partitionType;

    }

	ArrayList<SerializableWorkerMetadata> appendMetadataOfWorkers(final ArrayList<WorkerMeta> workers) {
		final ArrayList<SerializableWorkerMetadata> listSerializableWorkerMetadata = new ArrayList<>();
		for (final WorkerMeta worker : workers) {
			listSerializableWorkerMetadata.add(new SerializableWorkerMetadata(worker));
		}
		return listSerializableWorkerMetadata;
	}

	ArrayList<SerializableDouble> getDriverProfilePercentage(final ArrayList<Double> percentages) {
		final ArrayList<SerializableDouble> list = new ArrayList<>();
		for (final Double percentage : percentages) {
			list.add(new SerializableDouble(percentage));
		}
		return list;
	}

	ArrayList<SerializableInt> getLightNodeIndex(final ArrayList<Node> nodes) {
		final ArrayList<SerializableInt> list = new ArrayList<>();
		for (final Node node : nodes) {
			list.add(new SerializableInt(node.index));
		}
		return list;
	}

	ArrayList<Serializable_GPS_Rectangle> getListRouteWindow(final ArrayList<double[]> windows) {
		final ArrayList<Serializable_GPS_Rectangle> list = new ArrayList<>();
		for (final double[] window : windows) {
			list.add(new Serializable_GPS_Rectangle(window[0], window[1], window[2], window[3]));
		}
		return list;
	}

	@Override
	public String toString() {
		return "Message_SW_Setup{" +
				"isNewEnvironment=" + isNewEnvironment +
				", startStep=" + startStep +
				", numRandomPrivateVehicles=" + numRandomPrivateVehicles +
				", numRandomTrams=" + numRandomTrams +
				", numRandomBuses=" + numRandomBuses +
				", externalRoutes=" + externalRoutes +
                ", indexNodesToAddLight=" + indexNodesToAddLight +
                ", indexNodesToRemoveLight=" + indexNodesToRemoveLight +
				", partitiontType=" + partitionType +
                ", workerName=" + workerName +
				'}';
	}


	public int generateHash() {
		int hashCode = 1;
        for (SerializableInt e : this.indexNodesToAddLight) {
            hashCode = 31 * hashCode + (e == null ? 0 : e.value);
        }

        for (SerializableInt e : this.indexNodesToRemoveLight) {
            hashCode = 31 * hashCode + (e == null ? 0 : e.value);
        }


//		for (SerializableWorkerMetadata e : this.metadataWorkers) {
//			hashCode = 31 * hashCode + (e == null ? 0 : e.address.hashCode()*e.name.hashCode()*e.port);
//			for (SerializableGridCell gridCell : e.gridCells) {
//				hashCode = 31 * hashCode + (gridCell == null ? 0 : gridCell.row*gridCell.column);
//			}
//		}

//		hashCode = 31 * hashCode + Boolean.hashCode(this.isNewEnvironment);
//		hashCode = 31 * hashCode + this.startStep;
//		hashCode = 31 * hashCode + this.partitionType.hashCode();
//		hashCode = 31 * hashCode + this.numRandomPrivateVehicles;
//		hashCode = 31 * hashCode + this.numRandomTrams;
//		hashCode = 31 * hashCode + this.numRandomBuses;
//        System.out.println("HASH CODE SW SETUP: " + hashCode);
		return hashCode;
	}
}


/*
public class Message_SW_Setup {
	public boolean isNewEnvironment;
	public int numWorkers;
	public int startStep;
	public int maxNumSteps;
	public double numStepsPerSecond;
	public int workerToServerReportStepGapInServerlessMode;
	public double periodOfTrafficWaitForTramAtStop;
	public ArrayList<SerializableDouble> driverProfileDistribution = new ArrayList<>();
	public double lookAheadDistance;
	public String trafficLightTiming;
	public boolean isVisualize;
	public int numRandomPrivateVehicles;
	public int numRandomTrams;
	public int numRandomBuses;
	public ArrayList<SerializableExternalVehicle> externalRoutes = new ArrayList<>();

	//Metadata of all workers

    public ArrayList<SerializableWorkerMetadata> metadataWorkers = new ArrayList<>();
    public boolean isServerBased;
    public String roadGraph;
    public String routingAlgorithm;

     //Index of nodes where traffic light is added by user
    public ArrayList<SerializableInt> indexNodesToAddLight = new ArrayList<>();

     //Index of nodes where traffic light is removed by user
    public ArrayList<SerializableInt> indexNodesToRemoveLight = new ArrayList<>();
    public boolean isAllowPriorityVehicleUseTramTrack;
    public String outputTrajectoryScope;
    public String outputRouteScope;
    public String outputTravelTimeScope;
    public boolean isOutputSimulationLog = false;
    public ArrayList<Serializable_GPS_Rectangle> listRouteSourceWindowForInternalVehicle = new ArrayList<>();
    public ArrayList<Serializable_GPS_Rectangle> listRouteDestinationWindowForInternalVehicle = new ArrayList<>();
    public ArrayList<Serializable_GPS_Rectangle> listRouteSourceDestinationWindowForInternalVehicle = new ArrayList<>();
    public boolean isAllowReroute = false;
    public boolean isAllowTramRule = true;
    public boolean isDriveOnLeft;
    public String inputOpenStreetMapFile;
    public int mapHashValue;
    public String partitionType;

    public Message_SW_Setup(Message_SW_Setup template, final WorkerMeta workerToReceiveMessage) {
        this.isNewEnvironment = template.isNewEnvironment;
        this.numWorkers = template.numWorkers;
        this.startStep = template.startStep;
        this.maxNumSteps = template.maxNumSteps;
        this.numStepsPerSecond = template.numStepsPerSecond;
        this.workerToServerReportStepGapInServerlessMode = template.workerToServerReportStepGapInServerlessMode;
        this.periodOfTrafficWaitForTramAtStop = template.periodOfTrafficWaitForTramAtStop;
        this.driverProfileDistribution = template.driverProfileDistribution;
        this.lookAheadDistance = template.lookAheadDistance;
        this.trafficLightTiming = template.trafficLightTiming;
        this.isVisualize = template.isVisualize;
        this.numRandomPrivateVehicles = workerToReceiveMessage.numRandomPrivateVehicles;
        this.numRandomTrams = workerToReceiveMessage.numRandomTrams;  // is it thread safe?
        this.numRandomBuses = workerToReceiveMessage.numRandomBuses;
        this.externalRoutes = workerToReceiveMessage.externalRoutes;
        this.metadataWorkers = template.metadataWorkers;
        this.isServerBased = template.isServerBased;
        this.roadGraph = template.roadGraph;
        this.routingAlgorithm = template.routingAlgorithm;
        this.indexNodesToAddLight = template.indexNodesToAddLight;
        this.indexNodesToRemoveLight = template.indexNodesToRemoveLight;
        this.isAllowPriorityVehicleUseTramTrack = template.isAllowPriorityVehicleUseTramTrack;
        this.outputTrajectoryScope = template.outputTrajectoryScope;
        this.outputRouteScope = template.outputRouteScope;
        this.outputTravelTimeScope = template.outputTravelTimeScope;
        this.isOutputSimulationLog = template.isOutputSimulationLog;
        this.listRouteSourceWindowForInternalVehicle = template.listRouteSourceWindowForInternalVehicle;
        this.listRouteDestinationWindowForInternalVehicle = template.listRouteDestinationWindowForInternalVehicle;
        this.listRouteSourceDestinationWindowForInternalVehicle = template.listRouteSourceDestinationWindowForInternalVehicle;
        this.isAllowReroute = template.isAllowReroute;
        this.isAllowTramRule = template.isAllowTramRule;
        this.isDriveOnLeft = template.isDriveOnLeft;
        this.inputOpenStreetMapFile = template.inputOpenStreetMapFile;
        this.mapHashValue = template.mapHashValue;
        this.partitionType = template.partitionType;
    }

    public Message_SW_Setup() {

    }

    public Message_SW_Setup(final ArrayList<WorkerMeta> workers, final WorkerMeta workerToReceiveMessage, final int step, final ArrayList<Node> nodesToAddLight,
                            final ArrayList<Node> nodesToRemoveLight, final int trafficHashVal) {
        isNewEnvironment = Settings.isNewEnvironment;
        numWorkers = Settings.numWorkers;
        startStep = step;
        maxNumSteps = Settings.maxNumSteps;
        numStepsPerSecond = Settings.numStepsPerSecond;
        workerToServerReportStepGapInServerlessMode = Settings.trafficReportStepGapInServerlessMode;
        periodOfTrafficWaitForTramAtStop = Settings.periodOfTrafficWaitForTramAtStop;
        driverProfileDistribution = getDriverProfilePercentage(Settings.driverProfileDistribution);
        lookAheadDistance = Settings.lookAheadDistance;
        trafficLightTiming = Settings.trafficLightTiming.name();
        isVisualize = Settings.isVisualize;
        metadataWorkers = appendMetadataOfWorkers(workers);
        numRandomPrivateVehicles = workerToReceiveMessage.numRandomPrivateVehicles;
        numRandomTrams = workerToReceiveMessage.numRandomTrams;
        numRandomBuses = workerToReceiveMessage.numRandomBuses;
        externalRoutes = workerToReceiveMessage.externalRoutes;
        isServerBased = Settings.isServerBased;
        if (Settings.isNewEnvironment) {
            if (Settings.isBuiltinRoadGraph) {
                roadGraph = "builtin";
            } else {
//				roadGraph = Settings.roadGraph;
                roadGraph = "loadityourself";
                inputOpenStreetMapFile = Settings.inputOpenStreetMapFile;
                mapHashValue = trafficHashVal;
            }
        } else {
            roadGraph = "";
        }
        routingAlgorithm = Settings.routingAlgorithm.name();
        isAllowPriorityVehicleUseTramTrack = Settings.isAllowPriorityVehicleUseTramTrack;
        indexNodesToAddLight = getLightNodeIndex(nodesToAddLight);
        indexNodesToRemoveLight = getLightNodeIndex(nodesToRemoveLight);
        outputTrajectoryScope = Settings.outputTrajectoryScope.name();
        outputRouteScope = Settings.outputRouteScope.name();
        outputTravelTimeScope = Settings.outputTravelTimeScope.name();
        isOutputSimulationLog = Settings.isOutputSimulationLog;
        listRouteSourceWindowForInternalVehicle = getListRouteWindow(Settings.listRouteSourceWindowForInternalVehicle);
        listRouteDestinationWindowForInternalVehicle = getListRouteWindow(
                Settings.listRouteDestinationWindowForInternalVehicle);
        listRouteSourceDestinationWindowForInternalVehicle = getListRouteWindow(
                Settings.listRouteSourceDestinationWindowForInternalVehicle);
        isAllowReroute = Settings.isAllowReroute;
        isAllowTramRule = Settings.isAllowTramRule;
        isDriveOnLeft = Settings.isDriveOnLeft;
        partitionType = Settings.partitionType;

    }

    ArrayList<SerializableWorkerMetadata> appendMetadataOfWorkers(final ArrayList<WorkerMeta> workers) {
        final ArrayList<SerializableWorkerMetadata> listSerializableWorkerMetadata = new ArrayList<>();
        for (final WorkerMeta worker : workers) {
            listSerializableWorkerMetadata.add(new SerializableWorkerMetadata(worker));
        }
        return listSerializableWorkerMetadata;
    }

    ArrayList<SerializableDouble> getDriverProfilePercentage(final ArrayList<Double> percentages) {
        final ArrayList<SerializableDouble> list = new ArrayList<>();
        for (final Double percentage : percentages) {
            list.add(new SerializableDouble(percentage));
        }
        return list;
    }

    ArrayList<SerializableInt> getLightNodeIndex(final ArrayList<Node> nodes) {
        final ArrayList<SerializableInt> list = new ArrayList<>();
        for (final Node node : nodes) {
            list.add(new SerializableInt(node.index));
        }
        return list;
    }

    ArrayList<Serializable_GPS_Rectangle> getListRouteWindow(final ArrayList<double[]> windows) {
        final ArrayList<Serializable_GPS_Rectangle> list = new ArrayList<>();
        for (final double[] window : windows) {
            list.add(new Serializable_GPS_Rectangle(window[0], window[1], window[2], window[3]));
        }
        return list;
    }

    @Override
    public String toString() {
        return "Message_SW_Setup{" +
                "isNewEnvironment=" + isNewEnvironment +
                ", numWorkers=" + numWorkers +
                ", startStep=" + startStep +
                ", maxNumSteps=" + maxNumSteps +
                ", numStepsPerSecond=" + numStepsPerSecond +
                ", workerToServerReportStepGapInServerlessMode=" + workerToServerReportStepGapInServerlessMode +
                ", periodOfTrafficWaitForTramAtStop=" + periodOfTrafficWaitForTramAtStop +
                ", driverProfileDistribution=" + driverProfileDistribution +
                ", lookAheadDistance=" + lookAheadDistance +
                ", trafficLightTiming='" + trafficLightTiming + '\'' +
                ", isVisualize=" + isVisualize +
                ", numRandomPrivateVehicles=" + numRandomPrivateVehicles +
                ", numRandomTrams=" + numRandomTrams +
                ", numRandomBuses=" + numRandomBuses +
                ", externalRoutes=" + externalRoutes +
                ", #metadataWorkers=" + metadataWorkers.size() +
                ", isServerBased=" + isServerBased +
                ", roadGraph='" + roadGraph + '\'' +
                ", routingAlgorithm='" + routingAlgorithm + '\'' +
                ", indexNodesToAddLight=" + indexNodesToAddLight +
                ", indexNodesToRemoveLight=" + indexNodesToRemoveLight +
                ", isAllowPriorityVehicleUseTramTrack=" + isAllowPriorityVehicleUseTramTrack +
                ", outputTrajectoryScope='" + outputTrajectoryScope + '\'' +
                ", outputRouteScope='" + outputRouteScope + '\'' +
                ", outputTravelTimeScope='" + outputTravelTimeScope + '\'' +
                ", isOutputSimulationLog=" + isOutputSimulationLog +
                ", listRouteSourceWindowForInternalVehicle=" + listRouteSourceWindowForInternalVehicle +
                ", listRouteDestinationWindowForInternalVehicle=" + listRouteDestinationWindowForInternalVehicle +
                ", listRouteSourceDestinationWindowForInternalVehicle=" + listRouteSourceDestinationWindowForInternalVehicle +
                ", isAllowReroute=" + isAllowReroute +
                ", isAllowTramRule=" + isAllowTramRule +
                ", isDriveOnLeft=" + isDriveOnLeft +
                ", inputOpenStreetMapFile='" + inputOpenStreetMapFile + '\'' +
                ", mapHashValue=" + mapHashValue +
                '}';
    }


    public int generateHash() {
        int hashCode = 1;

        for (SerializableDouble e : this.driverProfileDistribution) {
            hashCode = 31 * hashCode + (e == null ? 0 : (int) e.value);
        }

        for (SerializableWorkerMetadata e : this.metadataWorkers) {
            hashCode = 31 * hashCode + (e == null ? 0 : e.address.hashCode()*e.name.hashCode()*e.port);
            for (SerializableGridCell gridCell : e.gridCells) {
                hashCode = 31 * hashCode + (gridCell == null ? 0 : gridCell.row*gridCell.column);
            }
        }

        for (SerializableInt e : this.indexNodesToAddLight) {
            hashCode = 31 * hashCode + (e == null ? 0 : e.value);
        }

        for (SerializableInt e : this.indexNodesToRemoveLight) {
            hashCode = 31 * hashCode + (e == null ? 0 : e.value);
        }

        for (Serializable_GPS_Rectangle e : this.listRouteSourceWindowForInternalVehicle) {
            hashCode = 31 * hashCode + (e == null ? 0 : ((int) (e.minLat*e.minLon*e.maxLat*e.maxLon)*1000));
        }
        for (Serializable_GPS_Rectangle e : this.listRouteDestinationWindowForInternalVehicle) {
            hashCode = 31 * hashCode + (e == null ? 0 : ((int) (e.minLat*e.minLon*e.maxLat*e.maxLon)*1000));
        }
        for (Serializable_GPS_Rectangle e : this.listRouteSourceDestinationWindowForInternalVehicle) {
            hashCode = 31 * hashCode + (e == null ? 0 : ((int) (e.minLat*e.minLon*e.maxLat*e.maxLon)*1000));
        }

        hashCode = 31 * hashCode + Boolean.hashCode(this.isNewEnvironment);
        hashCode = 31 * hashCode + this.numWorkers;
        hashCode = 31 * hashCode + this.startStep;
        hashCode = 31 * hashCode + this.maxNumSteps;
        hashCode = 31 * hashCode + (int)(this.numStepsPerSecond*100);
        hashCode = 31 * hashCode + this.workerToServerReportStepGapInServerlessMode;
        hashCode = 31 * hashCode + (int)this.periodOfTrafficWaitForTramAtStop;
        hashCode = 31 * hashCode + (int)this.lookAheadDistance;
        hashCode = 31 * hashCode + this.trafficLightTiming.hashCode();
        hashCode = 31 * hashCode + Boolean.hashCode(this.isVisualize);
//		hashCode = 31 * hashCode + this.numRandomPrivateVehicles;
//		hashCode = 31 * hashCode + this.numRandomTrams;
//		hashCode = 31 * hashCode + this.numRandomBuses;
        hashCode = 31 * hashCode + Boolean.hashCode(this.isServerBased);
        String tmp = "";
        if (this.roadGraph.length() > 10)
            tmp = this.roadGraph.substring(0, 9);
        hashCode = 31 * hashCode + tmp.hashCode();
        hashCode = 31 * hashCode + Boolean.hashCode(this.isAllowPriorityVehicleUseTramTrack);
        hashCode = 31 * hashCode + this.outputTrajectoryScope.hashCode();
        hashCode = 31 * hashCode + this.outputRouteScope.hashCode();
        hashCode = 31 * hashCode + this.outputTravelTimeScope.hashCode();
        hashCode = 31 * hashCode + Boolean.hashCode(this.isOutputSimulationLog);
        hashCode = 31 * hashCode + Boolean.hashCode(this.isAllowReroute);
        hashCode = 31 * hashCode + Boolean.hashCode(this.isAllowTramRule);
        hashCode = 31 * hashCode + Boolean.hashCode(this.isDriveOnLeft);
        hashCode = 31 * hashCode + ((this.inputOpenStreetMapFile == null) ? 0 : this.inputOpenStreetMapFile.hashCode());
        hashCode = 31 * hashCode + this.mapHashValue;

        return hashCode;
    }
}

 */