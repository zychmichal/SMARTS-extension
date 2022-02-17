package processor.communication.message;

import common.Settings;
import processor.server.WorkerMeta;
import traffic.road.Node;

import java.util.ArrayList;

public class Message_SWM_Setup {

    public int numWorkers;
    public int maxNumSteps;
    public double numStepsPerSecond;
    public String partitionType;
    public int workerToServerReportStepGapInServerlessMode;
    public double periodOfTrafficWaitForTramAtStop;
    public ArrayList<SerializableDouble> driverProfileDistribution = new ArrayList<>();
    public double lookAheadDistance;
    public String trafficLightTiming;
    public boolean isVisualize;
    /**
     * Metadata of all workers
     */
    public boolean isServerBased;
    public String roadGraph;
    public String routingAlgorithm;

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
    public int numGlobalRandomBackgroundPrivateVehicles;
    public boolean oneSimulation;


    public Message_SWM_Setup(Message_SWM_Setup template) {
        this.numWorkers = template.numWorkers;
        this.maxNumSteps = template.maxNumSteps;
        this.numStepsPerSecond = template.numStepsPerSecond;
        this.workerToServerReportStepGapInServerlessMode = template.workerToServerReportStepGapInServerlessMode;
        this.periodOfTrafficWaitForTramAtStop = template.periodOfTrafficWaitForTramAtStop;
        this.driverProfileDistribution = template.driverProfileDistribution;
        this.lookAheadDistance = template.lookAheadDistance;
        this.trafficLightTiming = template.trafficLightTiming;
        this.isVisualize = template.isVisualize;
        this.isServerBased = template.isServerBased;
        this.roadGraph = template.roadGraph;
        this.routingAlgorithm = template.routingAlgorithm;
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
        this.oneSimulation = template.oneSimulation;
        this.numGlobalRandomBackgroundPrivateVehicles = template.numGlobalRandomBackgroundPrivateVehicles;
    }

    public Message_SWM_Setup() {

    }

    public Message_SWM_Setup(final ArrayList<Node> nodesToAddLight,
                            final ArrayList<Node> nodesToRemoveLight, final int trafficHashVal) {
        numWorkers = Settings.numWorkers;
        maxNumSteps = Settings.maxNumSteps;
        numStepsPerSecond = Settings.numStepsPerSecond;
        workerToServerReportStepGapInServerlessMode = Settings.trafficReportStepGapInServerlessMode;
        periodOfTrafficWaitForTramAtStop = Settings.periodOfTrafficWaitForTramAtStop;
        driverProfileDistribution = getDriverProfilePercentage(Settings.driverProfileDistribution);
        lookAheadDistance = Settings.lookAheadDistance;
        trafficLightTiming = Settings.trafficLightTiming.name();
        isVisualize = Settings.isVisualize;
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
        oneSimulation = Settings.oneSimulation;
        numGlobalRandomBackgroundPrivateVehicles = Settings.numGlobalRandomBackgroundPrivateVehicles;
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
        return "Message_SWM_Setup{" +
                ", numWorkers=" + numWorkers +
                ", maxNumSteps=" + maxNumSteps +
                ", numStepsPerSecond=" + numStepsPerSecond +
                ", workerToServerReportStepGapInServerlessMode=" + workerToServerReportStepGapInServerlessMode +
                ", periodOfTrafficWaitForTramAtStop=" + periodOfTrafficWaitForTramAtStop +
                ", driverProfileDistribution=" + driverProfileDistribution +
                ", lookAheadDistance=" + lookAheadDistance +
                ", trafficLightTiming='" + trafficLightTiming + '\'' +
                ", isVisualize=" + isVisualize +
                ", isServerBased=" + isServerBased +
                ", roadGraph='" + roadGraph + '\'' +
                ", routingAlgorithm='" + routingAlgorithm + '\'' +
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
                ", oneSimulation='" + oneSimulation + '\'' +
                ", numGlobalRandomBackgroundPrivateVehicles='" + numGlobalRandomBackgroundPrivateVehicles + '\'' +
                ", mapHashValue=" + mapHashValue +
                '}';
    }


    public int generateHash() {
        int hashCode = 1;

        for (SerializableDouble e : this.driverProfileDistribution) {
            hashCode = 31 * hashCode + (e == null ? 0 : (int) e.value);
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

        hashCode = 31 * hashCode + this.numWorkers;
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
        hashCode = 31 * hashCode + this.numGlobalRandomBackgroundPrivateVehicles;

        return hashCode;
    }
}
