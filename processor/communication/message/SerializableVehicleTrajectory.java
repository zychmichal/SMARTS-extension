package processor.communication.message;

public class SerializableVehicleTrajectory {
	public double timeStamp;
	public double lat;
	public double lon;

	public SerializableVehicleTrajectory() {
	}

	public SerializableVehicleTrajectory(double timeStamp, double lat, double lon) {
		this.timeStamp = timeStamp;
		this.lat = lat;
		this.lon = lon;
	}
}
