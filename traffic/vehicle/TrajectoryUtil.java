package traffic.vehicle;

import processor.communication.message.SerializableVehicleTrajectory;

import java.util.ArrayList;

public class TrajectoryUtil {

    public static ArrayList<VehicleTrajectory> parseReceivedTrajectories(final ArrayList<SerializableVehicleTrajectory> vehicleTrajectories) {
        final ArrayList<VehicleTrajectory> trajectories = new ArrayList<>();
        for (final SerializableVehicleTrajectory trajectory : vehicleTrajectories) {
            trajectories.add(new VehicleTrajectory(trajectory.timeStamp, trajectory.lat, trajectory.lon));
        }
        return trajectories;
    }

}
