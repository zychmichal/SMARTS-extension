package processor.communication;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.communication.message.MessageUtil;

import static java.lang.System.exit;

public class MessageSender {
	public String address;
	public int port;
	public Socket socket;
	PrintWriter out = null;
	MessageUtil messageUtil = new MessageUtil();
	private static Logger logger;

	public MessageSender(final String receiverAddress, final int receiverPort, String ownerName){
		logger = ownerName.toLowerCase().equals("server") ? LogManager.getLogger("ServerLogger") : LogManager.getLogger("WorkerLogger");
		try {
			int tryNumber = 0;
			while (true) {
				if (tryNumber++ > 10){
					System.err.println("[ERROR][IMPORTANT] Connection with " + receiverAddress + ":" + receiverPort + " could not be established!");
					exit(-1);
				}

				try { // 0 1 4 9 16 25 36 49 64 81 100
					if (tryNumber>0)
						Thread.sleep(200 * tryNumber * tryNumber);

					socket = new Socket(receiverAddress, receiverPort);
					break;
				} catch (ConnectException e) {
					if (tryNumber >= 2)
						logger.warn(ownerName + " [MessageSender] tried " + tryNumber + " times to connect with " + receiverAddress +":" + receiverPort);
                        System.err.println(ownerName + " [MessageSender] tried " + tryNumber + " times to connect with " + receiverAddress +":" + receiverPort);
					if (tryNumber > 1)
						System.err.println("Reconnect #try " + tryNumber);
					continue;
				} catch (InterruptedException e) {
					e.printStackTrace();
					logger.error(ownerName + " [MessageSender] --- InterruptedException ", e);
				}
			}

			socket.setTcpNoDelay(true);
			out = new PrintWriter(socket.getOutputStream(), true);

		} catch (final UnknownHostException e) {
			e.printStackTrace();
			logger.error(ownerName + " [MessageSender] --- UnknownHostException ", e);
		} catch (final IOException e) {
			e.printStackTrace();
			logger.error(ownerName + " [MessageSender] --- IOException ", e);
		}
		address = receiverAddress;
		port = receiverPort;
	}

	public void send(final Object message) {
		out.println(messageUtil.compose(message));
	}
}

