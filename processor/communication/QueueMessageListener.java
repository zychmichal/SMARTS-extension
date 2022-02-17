package processor.communication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;

/**
 * This class passes the received messages to message handler for processing.
 *
 */
public class QueueMessageListener implements Runnable {
//	class MessageProcessRunnable implements Runnable {
//        private final BlockingQueue<String> queue;
//        private String name;
////        private AtomicBoolean running = new AtomicBoolean(true);
//
//		public MessageProcessRunnable(BlockingQueue<String> queue, String name) {
//            this.queue = queue;
//            this.name = name;
//		}
//
//		public void run() {
//            try {
//                while (true){              //while(running.get()){
//                    String msg = queue.take();
//                    if (msg.equals(Settings.close_pill))
//                        break;
//                    final Object received = MessageUtil.read(msg);
//                    messageHandler.processReceivedMsg(received);
//                }
//            } catch (InterruptedException e) {
//                System.err.println("["+name + "][MessageProcessRunnable-Thread][PARSING-THREAD]  --  interrupted");
//                e.printStackTrace();
//            } catch (Exception e) {
//                System.err.println("["+name + "][MessageProcessRunnable-Thread][PARSING-THREAD]  --  UknownException");
//                e.printStackTrace();
//            }
//		}
//	}

	Socket socket = null;
	BufferedReader bufferedReader;
	InputStream inputStream;
	InputStreamReader inputStreamReader;
	MessageHandler messageHandler = null;
	final String ownerName;
	private final BlockingQueue<String> queue;
	private Logger logger = null;

	boolean running = true;
	boolean stopped = false;

	/**
	 * @param socket
	 *            An established connection.
	 * @param messageHandler
	 *            Entity that processes received messages.
	 */
	public QueueMessageListener(final String name, final Socket socket, final MessageHandler messageHandler, BlockingQueue<String> queue) {
		this.socket = socket;
		this.messageHandler = messageHandler;
		this.ownerName = name;
		this.queue = queue;
		if (ownerName.toLowerCase().equals("server")){
			this.logger = LogManager.getLogger("ServerLogger");
		} else {
			this.logger = LogManager.getLogger("WorkerLogger");
			logger.info("New listning connection on " + socket.toString());
		}
	}

	/**
	 * Constantly listen for messages. When a message arrives, starts a new
	 * thread to handle it.
	 */
	@Override
	public void run() {
		try {
			inputStream = socket.getInputStream();
			inputStreamReader = new InputStreamReader(inputStream);
			bufferedReader = new BufferedReader(inputStreamReader);
			logger.debug("Starting socket connection on " + socket.toString());

			String nextLine;
//            Thread MessageProcessRunnable = new Thread(new MessageProcessRunnable(queue, ownerName), "["+ ownerName+"][PARSING-THREAD]");
//            MessageProcessRunnable.start();
			while (running && ((nextLine = bufferedReader.readLine()) != null)) {
				logger.debug("Puting msg into queue");
				logger.debug(nextLine);
				queue.put(nextLine);

//				final Thread thread = new Thread(new MessageProcessRunnable(nextLine), "["+ ownerName+"][PARSING-THREAD]");
//				thread.start();
			}
		} catch (final InterruptedException e) {
			System.err.println("["+ownerName + "][INTERUPTED_EXCEPTION][QueueMsgListenerBlocking]  --- interupted");
			e.printStackTrace();
			logger.error("EXCEPTION --- InterruptedException in QueueMessageListener: ", e);
		} catch (final SocketException e) {
			if (stopped){
				logger.warn("QueueMessageListener closed: " + ownerName + " socket: " + socket.toString());
				stopped = false;
			}
			else {
				System.err.println("[" + ownerName + "][SocketException][QueueMsgListenerBlocking]");
				e.printStackTrace();
				logger.error("EXCEPTION --- SocketException in QueueMessageListener: ", e);
			}
		} catch (final IOException e) {
			System.err.println("["+ownerName + "][IOException][QueueMsgListenerBlocking]");
			e.printStackTrace();
			logger.error("EXCEPTION --- IOException in QueueMessageListener: ", e);
		} catch (final Exception e) {
			System.err.println("["+ownerName + "][IGNORED][QueueMsgListenerBlocking]");
			e.printStackTrace();
			logger.error("EXCEPTION --- Unknown Exception in QueueMessageListener: ", e);
		} finally {
			terminate();
			System.exit(1000);
		}
	}

	public void preterminate(){
		stopped = true;
	}

	public void terminate() {
		try {
			running = false;
			bufferedReader.close();
			inputStreamReader.close();
			inputStream.close();
			socket.close();
		} catch (final Exception e) {
			e.printStackTrace();
			logger.error("EXCEPTION --- while terminating QueueMessageListener: ", e);
		}

	}
}
