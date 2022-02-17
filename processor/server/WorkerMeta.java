package processor.server;

import java.util.ArrayList;

import processor.communication.MessageSender;
import processor.communication.message.SerializableExternalVehicle;
import processor.worker.Workarea;
import traffic.road.GridCell;

/**
 * Meta data of worker.
 */
public class WorkerMeta {
    public String workerManagerName;
    public int indexWorkerManager;
	public String name;
	public String address;
	public int port;
	public Workarea workarea;
	public double laneLengthRatioAgainstWholeMap;
	public MessageSender sender;
	public int numRandomPrivateVehicles;
	public int numRandomTrams;
	public int numRandomBuses;
	public WorkerState state = WorkerState.NEW;
	public ArrayList<SerializableExternalVehicle> externalRoutes = new ArrayList<>();

	/**
	 *
	 * @param name
	 *            Name of the worker.
	 * @param address
	 *            Network address of the worker.
	 * @param port
	 *            Worker's port for receiving messages from server.
	 */
	public WorkerMeta(final String name, final String address, final int port, String ownerName) {
		this.name = name;
		workarea = new Workarea(name, new ArrayList<GridCell>());
        this.address = address;
        this.port = port;
		sender = new MessageSender(address, port, ownerName);
	}

    public WorkerMeta(final String workerManagerName, final int indexWorkerManager, final String name, final String address, final int port) {
        this.name = name;
        workarea = new Workarea(name, new ArrayList<GridCell>());
        //sender = new MessageSender(address, port, ownerName);
        this.address = address;
        this.port = port;
        this.workerManagerName = workerManagerName;
        this.indexWorkerManager = indexWorkerManager;
    }

	public void send(final Object message) {
		sender.send(message);
	}

	public synchronized void setState(final WorkerState state) {
		this.state = state;
	}

	public WorkerState getState() {
		return this.state;
	}

	public synchronized boolean isEqual(final WorkerState state) {
		return this.state == state;
	}
}
