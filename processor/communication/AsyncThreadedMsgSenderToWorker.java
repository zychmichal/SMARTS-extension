package processor.communication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.communication.message.Message_SW_Setup;
import processor.server.WorkerMeta;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static processor.communication.SendMessageType.Message_SW_KillWorker;

public class AsyncThreadedMsgSenderToWorker {
    private <T> ArrayList<ArrayList<T>> chopped(ArrayList<T> list, int numberOfSublists) {
        final int N = list.size();
        // to iterate by minimum 1
        int L = Math.max(N/numberOfSublists, 1);
        if (N%numberOfSublists!=0){
            if (N>numberOfSublists) L++;
            logger.warn("[SERVER][AsyncThreadedMsgSender] - number of WorkerMetas sub-lists is not divisible by " + numberOfSublists);
            System.err.println("[SERVER][AsyncThreadedMsgSender] - number of WorkerMetas sub-lists is not divisible by " + numberOfSublists);
        }
        ArrayList<ArrayList<T>> parts = new ArrayList<ArrayList<T>>();

        for (int i = 0; i < N; i += L) {
            final int iNext = i + L;
            parts.add(new ArrayList<T>(list.subList(i, Math.min(N, iNext))));
        }

        for (ArrayList<T> part : parts) {
            if (parts.get(0).size() != part.size()) {
                logger.warn("[SERVER][AsyncThreadedMsgSender] - WorkerMetas list is not equally divided between threads.");
                System.err.println("[SERVER][AsyncThreadedMsgSender] - WorkerMetas list is not equally divided between threads.");
            }
        }
        return parts;
    }


	ArrayList<WorkerMeta> workerMetas;
	ArrayList<ArrayList<WorkerMeta>> dividedWorkerMetas;
	private int numberOfWorkers;
	private int numberOfSendingUnits;
	ThreadPoolExecutor executor;
	private static Logger logger;


	// Used only by server to sync sending messages to all workers in workerMetas
	public AsyncThreadedMsgSenderToWorker(ArrayList<WorkerMeta> workerMetas){

		logger = LogManager.getLogger("ServerLogger");
		this.workerMetas = workerMetas;
		this.numberOfWorkers = workerMetas.size();
		this.numberOfSendingUnits = Math.min(numberOfWorkers, 24);
        this.dividedWorkerMetas = chopped(workerMetas, numberOfSendingUnits);
        if (dividedWorkerMetas.size() != numberOfSendingUnits) {
            logger.error("[SERVER][AsyncThreadedMsgSender] - dividedWorkerMetas.size() is not equal to numberOfSendingUnits.");
            System.err.println("[SERVER][AsyncThreadedMsgSender] - dividedWorkerMetas.size() is not equal to numberOfSendingUnits.");
            numberOfSendingUnits = dividedWorkerMetas.size();
        }
        this.executor =  new ThreadPoolExecutor(numberOfSendingUnits, numberOfSendingUnits,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        logger.info("AsyncThreadedMsgSender() initialized: numberOfWorkers=" + numberOfWorkers
                    + ", numberOfSendingUnits=" + numberOfSendingUnits);
	}

    public void sendOutToAllWorkers(Message_SW_Setup template, SendMessageType msgType, int messageHash) {
        if (executor.getActiveCount() != 0) {
            logger.warn("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
            System.err.println("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
        }

        Thread t = Thread.currentThread();
        logger.info("Thread: " + t.getName() + ", hash of SW_Setup message: " + messageHash);


        if (workerMetas.size() > 0) {
            logger.info("[SERVER][AsyncThreadedMsgSender] - msgType: " + msgType.toString()
                    + ", send by: " + numberOfSendingUnits
                    + ", task Count: " + executor.getTaskCount()
                    + ", completedTaskCount: " + executor.getCompletedTaskCount());

            for (int i = 0; i < numberOfSendingUnits; ++i) {
                executor.execute(
                        new SendMessagesUnit(
                                template,
                                i,
                                dividedWorkerMetas.get(i),
                                msgType,
                                messageHash)
                );
            }

        }
    }

    public void sendOutToAllWorkers(SendMessageType msgType, int step) {
        if (executor.getActiveCount() != 0) {
            logger.warn("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
            System.err.println("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
        }

        if (workerMetas.size() > 0) {
            logger.info("[SERVER][AsyncThreadedMsgSender] - msgType: " + msgType.toString()
                    + ", send by: " + numberOfSendingUnits
                    + ", task Count: " + executor.getTaskCount()
                    + ", completedTaskCount: " + executor.getCompletedTaskCount());

            for (int i = 0; i < numberOfSendingUnits; ++i) {
                executor.execute(
                        new SendMessagesUnit(
                                i,
                                dividedWorkerMetas.get(i),
                                msgType,
                                step)
                );
            }

            if (msgType.equals(Message_SW_KillWorker)){
                executor.shutdown();
                logger.info("[SERVER] - [AsyncThreadedMsgSender] waiting up to 10sec for SendMessagesUnit to send all kill messages and shutdown..." );
                boolean succesfull = true;
                try {
                    executor.awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    succesfull = false;
                    logger.error("Killing InterruptedException while waiting for shutdown - threads were not able to send messages in 10 sec.");
                    System.err.println("Killing InterruptedException while waiting for shutdown.");
                    e.printStackTrace();
                    executor.shutdownNow();
                }
                if (succesfull)
                    logger.info("[SERVER] - [AsyncThreadedMsgSender] all [SendMessagesUnit] threads died." );

            }
        }
    }


//
//
//    public void sendOutToAllWorkers(ArrayList<WorkerMeta> workerMetas, WorkerMeta worker, int step, ArrayList<Node> nodesToAddLight, ArrayList<Node> nodesToRemoveLight, int trafficHashVal, SendMessageType msgType) {
//        if (executor.getActiveCount() != 0) {
//            logger.warn("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
//                    + " and tries to send out " + msgType.toString());
//            System.err.println("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
//                    + " and tries to send out " + msgType.toString());
//        }
//
//        Thread t = Thread.currentThread();
//        logger.info("HEJO name: " + t.getName() + ", state: " + t.getState()); //TODO DELETE THIS
//
//        Message_SW_Setup msg = new Message_SW_Setup(workerMetas, worker, step, nodesToAddLight, nodesToRemoveLight, trafficHashVal);
//
//        logger.info(msg.toString());
//
//        if (workerMetas.size() > 0) {
//            logger.info("[SERVER][AsyncThreadedMsgSender] - msgType: " + msgType.toString()
//                    + ", send by: " + numberOfSendingUnits
//                    + ", task Count: " + executor.getTaskCount()
//                    + ", completedTaskCount: " + executor.getCompletedTaskCount());
//
//            for (int i = 0; i < numberOfSendingUnits; ++i) {
//                executor.execute(
//                        new SendMessagesUnit(
//                                msg,
//                                i,
//                                dividedWorkerMetas.get(i),
//                                msgType)
//                );
//            }
//
//            logger.info("Executed - to delete this info"); //TODO DELETE THIS
//        }
//	}


//	public void runAllSenders() {
//		try {
//			for (int i = 0; i < numberOfSendingUnits; ++i){
//				new Thread(
//						threadGroup,
//						new MessageProcessingUnit(messageHandler, queue, "Server", threadGroup),
//						"[Server]" + threadGroup.getName()+"[T_" + i + "]"
//				).start();
//			}
//		} catch (final Exception e) {
//			System.err.println("[Server][" + threadGroup.getName() + "]");
//			e.printStackTrace();
//			logger.error("EXCEPTION in AsyncThreadedMsgSender.runAllSenders() owner: Server --- Unknown Exception: ", e);
//		}
//	}
}



