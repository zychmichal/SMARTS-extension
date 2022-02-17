package processor.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import common.Settings;
import common.SysUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.communication.message.SerializableRoute;
import processor.communication.message.SerializableRouteDumpPoint;
import traffic.routing.Routing;

public class FileOutput {
	FileOutputStream fosLog;
	FileOutputStream fosTrajectory;
	FileOutputStream fosRoute;
	FileOutputStream fosTravelTime;

	private static Logger rootLogger = LogManager.getRootLogger();

	/**
	 * Close output file
	 */
	void close() {
		try {
			if (fosLog != null) {
				fosLog.close();
			}
			if (fosTrajectory != null) {
				fosTrajectory.close();
			}
			if (fosRoute != null) {
				outputStringToFile(fosRoute, "</data>");
				fosRoute.close();
			}
			if (fosTravelTime != null) {
				fosTravelTime.close();
			}
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			rootLogger.error("IOException in FileOutput.close()", e);
		}
	}

	void init() {
		if (Settings.outputTrajectoryScope != DataOutputScope.NONE) {
			initTrajectoryOutputFile();
		}
		if (Settings.outputRouteScope != DataOutputScope.NONE) {
			initRouteOutputFile();
		}
		if (Settings.outputTravelTimeScope != DataOutputScope.NONE) {
			initTravelTimeOutputFile();
		}
		if (Settings.isOutputSimulationLog) {
			initSimLogOutputFile();
		}
	}

	File getNewFile(String prefix) {
		String fileName = prefix + SysUtil.getTimeStampString() + ".txt";
		File file = new File(fileName);
		int counter = 0;
		while (file.exists()) {
			counter++;
			fileName = prefix + SysUtil.getTimeStampString() + "_" + counter + ".txt";
			file = new File(fileName);
		}
		return file;
	}

	void initRouteOutputFile() {
		try {
			final File file = getNewFile(Settings.prefixOutputRoutePlan);

			File parent = file.getParentFile();
			if (!parent.exists() && !parent.mkdirs()) {
				throw new IllegalStateException("Couldn't create dir: " + parent);
			}

			fosRoute = new FileOutputStream(file, true);
			outputStringToFile(fosRoute,
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + System.getProperty("line.separator"));
			outputStringToFile(fosRoute, "<data>" + System.getProperty("line.separator"));
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			rootLogger.error("IOException in FileOutput.initRouteOutputFile()", e);
		}
	}

	void initTravelTimeOutputFile() {
		try {
			final File file = getNewFile(Settings.prefixOutputTravelTime);

			File parent = file.getParentFile();
			if (!parent.exists() && !parent.mkdirs()) {
				throw new IllegalStateException("Couldn't create dir: " + parent);
			}
			// Print column titles
			fosTravelTime = new FileOutputStream(file, true);
			outputStringToFile(fosTravelTime, "Vehicle ID,Travel Time" + System.getProperty("line.separator"));
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			rootLogger.error("IOException in FileOutput.initTravelTimeOutputFile()", e);
		}
	}

	void initSimLogOutputFile() {
		try {
			final File file = getNewFile(Settings.prefixOutputSimLog);

			File parent = file.getParentFile();
			if (!parent.exists() && !parent.mkdirs()) {
				throw new IllegalStateException("Couldn't create dir: " + parent);
			}

			// Print column titles
			fosLog = new FileOutputStream(file, true);
			outputStringToFile(fosLog,
					"Time Stamp, Real Time (s), Simulation Time (s), # of Worker-Worker Connections, Average Travel Speed (km/h)"
							+ System.getProperty("line.separator"));
		} catch (final Exception e) {
			e.printStackTrace();
			rootLogger.error("IOException in FileOutput.initSimLogOutputFile()", e);
		}
	}

	void initTrajectoryOutputFile() {
		try {
			final File file = getNewFile(Settings.prefixOutputTrajectory);
			// Print column titles
			fosTrajectory = new FileOutputStream(file, true);
			outputStringToFile(fosTrajectory,
					"Trajectory ID,Vehicle ID,Vehicle Type,Time Stamp,Latitude,Longitude" + System.getProperty("line.separator"));
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			rootLogger.error("IOException in FileOutput.initTrajectoryOutputFile()", e);
		}
	}

	void outputTravelTime(HashMap<String, Double> records) {
		if (fosTravelTime != null) {
			try {
				final StringBuilder sb = new StringBuilder();
				for (final Entry<String, Double> record : records.entrySet()) {
					String vehicleId = record.getKey();
					double travelTime = record.getValue();
					sb.append(vehicleId);
					sb.append(",");
					sb.append(travelTime);
					sb.append(System.getProperty("line.separator"));
				}
				outputStringToFile(fosTravelTime, sb.toString());
			} catch (final Exception e) {
				e.printStackTrace();
				rootLogger.error("Exception in FileOutput.outputTravelTime()", e);
			}
		}
	}

	/**
	 * Output trajectory of individual vehicles
	 */
	void outputTrajectories(HashMap<String, TreeMap<Double, double[]>> allTrajectories) {
		if (fosTrajectory != null) {
			int trajectoryId = 0;
			for (String key : allTrajectories.keySet()) {
				// Trajectory counter
				trajectoryId++;
				outputStringToFile(fosTrajectory, String.valueOf(trajectoryId));
				outputStringToFile(fosTrajectory, ",");
				String[] itemsInKey=key.split("_");
				String vehicleId=itemsInKey[0];
				String vehileType=itemsInKey[1];
				outputStringToFile(fosTrajectory, vehicleId);
				outputStringToFile(fosTrajectory, ",");
				outputStringToFile(fosTrajectory, vehileType);
				outputStringToFile(fosTrajectory, ",");
				TreeMap<Double, double[]> points = allTrajectories.get(key);
				ArrayList<Double> timeStamps = new ArrayList<Double>(points.keySet());
				for (int i = 0; i < timeStamps.size(); i++) {
					if (i > 0) {
						outputStringToFile(fosTrajectory, ",,,");
					}
					double timeStamp = timeStamps.get(i);
					outputStringToFile(fosTrajectory, String.valueOf(timeStamp));
					outputStringToFile(fosTrajectory, ",");
					double[] point = points.get(timeStamp);
					outputStringToFile(fosTrajectory, String.valueOf(point[0]));
					outputStringToFile(fosTrajectory, ",");
					outputStringToFile(fosTrajectory, String.valueOf(point[1]));
					outputStringToFile(fosTrajectory, System.getProperty("line.separator"));

				}
			}
		}
	}

	void outputRoutes(final ArrayList<SerializableRoute> routes) {
		if (fosRoute != null) {
			try {
				final StringBuilder sb = new StringBuilder();
				for (final SerializableRoute route : routes) {
					sb.append("<vehicle ");
					sb.append("id=\"" + route.vehicleId + "\" type=\"" + route.type + "\" start_time=\""
							+ route.startTime + "\" driverProfile=\"" + route.driverProfile + "\">"
							+ System.getProperty("line.separator"));
					for (final SerializableRouteDumpPoint point : route.routeDumpPoints) {
						sb.append("<node ");
						if (point.stopDuration == 0) {
							sb.append("id=\"" + point.nodeId + "\"/>" + System.getProperty("line.separator"));
						} else {
							sb.append("id=\"" + point.nodeId + "\" stopover=\"" + point.stopDuration + "\"/>"
									+ System.getProperty("line.separator"));
						}
					}
					sb.append("</vehicle>" + System.getProperty("line.separator"));

				}
				outputStringToFile(fosRoute, sb.toString());
			} catch (final Exception e) {
				e.printStackTrace();
				rootLogger.error("Exception in FileOutput.outputRoutes()", e);
			}
		}
	}

	void outputSimLog(final int stepCurrent, final double simulationTimeCounter, final int totalNumFellowsOfWorker,
					  final double vehicleCount, final double aggregatedSpeedValue) {
		final Date date = new Date();

		if (fosLog != null) {
			outputStringToFile(fosLog, date.toString());
			outputStringToFile(fosLog, ",");
			outputStringToFile(fosLog, String.valueOf(stepCurrent / Settings.numStepsPerSecond));
			outputStringToFile(fosLog, ",");
			outputStringToFile(fosLog, String.valueOf(simulationTimeCounter));
			outputStringToFile(fosLog, ",");
			outputStringToFile(fosLog, String.valueOf(totalNumFellowsOfWorker));
			outputStringToFile(fosLog, ",");
			outputStringToFile(fosLog, String.valueOf(aggregatedSpeedValue / vehicleCount * 3.6));
			outputStringToFile(fosLog, System.getProperty("line.separator"));
		}
	}

	void outputStringToFile(final FileOutputStream fos, final String str) {

		final byte[] dataInBytes = str.getBytes();

		try {
			fos.write(dataInBytes);
			fos.flush();
		} catch (final IOException e) {
			e.printStackTrace();
			rootLogger.error("IOException in FileOutput.outputStringToFile()", e);
		}
	}

	public static DataOutputScope getScopeFromString(String selected) {
		for (final DataOutputScope scope : DataOutputScope.values()) {
			if (selected.equalsIgnoreCase(scope.name())) {
				return scope;
			}
		}
		return DataOutputScope.NONE;
	}
}
