package processor.communication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;

/**
 * Listens connection request on a given port and builds socket connections upon
 * request. When a new connection is established, a new thread will be created
 * for processing messages sent from the other end of the connection. This class
 * builds connections for server-worker and worker-worker communications.
 *
 */
public class IncomingConnectionBuilder extends Thread {
	int port;
	MessageHandler messageHandler;
	boolean running = true;
	ServerSocket incomingConnectionRequestSocket;
	ArrayList<QueueMessageListener> incomingConnections = new ArrayList<QueueMessageListener>();

	private final String ownerName;
	MessageProcessor messageProcessor;
	private static Logger logger = null;

	/**
	 *	@param messageProcessor
	 *				Message processing manager - initializes queue all together with threads for parsing incoming messages
	 *	@param port
	 *				Incomming sockets connections port number - used to initialize connection channel with
	 */
	public IncomingConnectionBuilder(MessageProcessor messageProcessor, final int port, final MessageHandler messageHandler, ServerSocket ss) {
		this.ownerName = messageProcessor.getOwnerName();
		this.port = port;
		this.messageHandler = messageHandler;
		this.messageProcessor = messageProcessor;
		logger = ownerName.toLowerCase().equals("server") ? LogManager.getLogger("ServerLogger") : LogManager.getLogger("WorkerLogger");
		try {
			if (ownerName.toLowerCase().equals("server")) {
				incomingConnectionRequestSocket = new ServerSocket();
				incomingConnectionRequestSocket.bind(new InetSocketAddress(port));
			}
			else
				incomingConnectionRequestSocket = ss;
		} catch (IOException e) {
			System.err.println("[" + ownerName + "] --- ConnectionBuilder (IOException) in Constructor IncomingConnectionBuilder().");
			e.printStackTrace();
			logger.error("WARNING [" + ownerName + "] --- ConnectionBuilder (IOException) in Constructor IncomingConnectionBuilder(). ", e);

		}
	}

	@Override
	public void run() {
		try {
			logger.info("Starting incoming connection builder on " + port);

			int listenerIndex = 0;
			while (running) {
				final Socket newSocket = incomingConnectionRequestSocket.accept();

				final QueueMessageListener messageListener = new QueueMessageListener(ownerName, newSocket, messageHandler, messageProcessor.getQueue());
				incomingConnections.add(messageListener);
				final Thread thread = new Thread(
						messageListener,
						"["+ ownerName+"][QListener][CH_" + (listenerIndex++) +"]"
				);
				thread.start();
			}
		} catch (final SocketException socketException) {
			System.err.println("[" + ownerName + "] --- ConnectionBuilder thread ends.");
			socketException.printStackTrace();
			logger.error("WARNING [" + ownerName + "] --- ConnectionBuilder thread ends. ", socketException);
			return;
		} catch (final Exception e) {
			System.err.println("[" + ownerName + "] --- Unknown exception in IncomingConnectionBuilder");
			e.printStackTrace();
			logger.error("EXCEPTION ["  + ownerName + "] --- Unknown exception in IncommingConnectionBuilder: ", e);
		} finally{
			terminate();
		}
	}

	public void preTerminateChannels(){
		for (final QueueMessageListener listener : incomingConnections) {
			listener.preterminate();
		}
	}


	public void terminate() {
		try {
			running = false;
			for (final QueueMessageListener listener : incomingConnections) {
				listener.terminate();
			}
			incomingConnectionRequestSocket.close();
		} catch (final Exception e) {
			e.printStackTrace();
			logger.error("EXCEPTION --- while terminating IncommingConnectionBuilder: ", e);
		}
	}

	@Override
	public String toString() {
		return "IncomingConnectionBuilder{" +
				"ownerName= " + ownerName +
				", port=" + port +
				", #incomingConnections=" + incomingConnections.size() +
				", incomingConnectionRequestSocket=" + incomingConnectionRequestSocket +
				'}';
	}
}
