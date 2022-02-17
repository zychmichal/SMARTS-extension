package processor.worker;

import common.Settings;
import common.SysUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import traffic.vehicle.Vehicle;
import traffic.vehicle.VehicleTrajectory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class FileOutputWorker {
	FileOutputStream fosTrajectory;

	private static Logger logger;

	/**
	 * Close output file
	 */
	void close() {
		try {
			if (fosTrajectory != null) {
				fosTrajectory.close();
			}
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("IOException in worker in FileOutput.close()", e);
		}
	}

	void init(String workerName) {
		workerName = "results/trajectories/" + workerName.split(":")[0] +"/" + workerName.replace(":","-");
		if (Settings.collectTrajectoryInWorkers) {
			initTrajectoryOutputFile(workerName);
		}
	}

	File getNewFile(String prefix) {
		String fileName = prefix  + "___" + SysUtil.getTimeStampString() + ".txt";
		File file = new File(fileName);
		int counter = 0;
		while (file.exists()) {
			counter++;
			fileName = prefix + "___" + SysUtil.getTimeStampString() + "_" + counter + ".txt";
			file = new File(fileName);
		}
		return file;
	}


	void initTrajectoryOutputFile(String prefix) {
		try {
			final File file = getNewFile(prefix);

			File parent = file.getParentFile();
			for (int i=1; i<5 && !parent.exists(); i++ ) {
				if (!parent.exists() && !parent.mkdirs()) {
					try {
						throw new IllegalStateException("Couldn't create dir: " + parent);
					} catch (IllegalStateException e) {
						logger.warn("Try #" +i +" - FileOutputWorker.initTrajectoryOutputFile() - " + prefix + " - IllegalStateException: retrying...");
						if (i>=3)
							logger.error("Try #" +i +" - FileOutputWorker.initTrajectoryOutputFile() - "  + prefix + " - IllegalStateException: cannot create directory path for file ...");
					}
				}
			}

			// Print column titles
			fosTrajectory = new FileOutputStream(file, true);
			outputStringToFile(fosTrajectory,
					"Trajectory ID,Vehicle ID,Vehicle Type,Time Stamp,Latitude,Longitude" + System.getProperty("line.separator"));
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("IOException in FileOutput.initTrajectoryOutputFile()", e);
		}
	}

	/**
	 * Output trajectory of individual vehicles
	 */
	void outputTrajectories(ArrayList<Vehicle> vehicleArrayList) {
		if (fosTrajectory != null) {
			int trajectoryId = 0;
			for (Vehicle vehicle : vehicleArrayList){
				// Trajectory counter
				trajectoryId++;
				outputStringToFile(fosTrajectory, String.valueOf(trajectoryId));
				outputStringToFile(fosTrajectory, ",");
				String vehicleId=vehicle.id;
				String vehileType=vehicle.type.name();
				outputStringToFile(fosTrajectory, vehicleId);
				outputStringToFile(fosTrajectory, ",");
				outputStringToFile(fosTrajectory, vehileType);
				outputStringToFile(fosTrajectory, ",");

				for (int i = 0; i < vehicle.trajectories.size(); i++){
					if (i > 0) {
						outputStringToFile(fosTrajectory, ",,,");
					}
					final VehicleTrajectory vTrajectory = vehicle.trajectories.get(i);
					double timeStamp = vTrajectory.timeStamp;
					outputStringToFile(fosTrajectory, String.valueOf(timeStamp));
					outputStringToFile(fosTrajectory, ",");
//					double[] point = points.get(timeStamp);
					outputStringToFile(fosTrajectory, String.valueOf(vTrajectory.lat));
					outputStringToFile(fosTrajectory, ",");
					outputStringToFile(fosTrajectory, String.valueOf(vTrajectory.lon));
					outputStringToFile(fosTrajectory, System.getProperty("line.separator"));

				}
			}
		}
	}

	void outputStringToFile(final FileOutputStream fos, final String str) {

		final byte[] dataInBytes = str.getBytes();

		try {
			fos.write(dataInBytes);
			fos.flush();
		} catch (final IOException e) {
			e.printStackTrace();
			logger.error("IOException in FileOutput.outputStringToFile()", e);
		}
	}

	public FileOutputWorker() {
		logger = LogManager.getLogger("WorkerLogger");
	}
}
