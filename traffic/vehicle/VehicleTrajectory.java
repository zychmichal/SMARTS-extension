package traffic.vehicle;

/**
 * This class contains information of vehicle completed trajectories (its position in timestamp)
 * It is exchanged between workers - used in WW_Traffic messages
 * Each Vehicle trajectory is bounded to specific vehicle (meaning when vehicle is passed to another worker, its trajectory is passed with it)
 * Created on 26/01/2021
 */
public class VehicleTrajectory {
    public double timeStamp;
    public double lat;
    public double lon;

    public VehicleTrajectory(double timeStamp, double lat, double lon) {
        this.timeStamp = timeStamp;
        this.lat = lat;
        this.lon = lon;
    }
}
