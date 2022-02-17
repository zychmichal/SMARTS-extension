package processor.communication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.communication.message.Message_SWM_CreateWorkers;
import processor.communication.message.Message_SWM_Setup;
import processor.communication.message.Message_SWM_WorkerSetup;
import processor.communication.message.Message_SW_Setup;
import processor.server.WorkerManagerMeta;
import processor.server.WorkerMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static processor.communication.SendMessageType.Message_SW_KillWorker;

public class AsyncThreadedMsgSenderToWorkerManager {
    private <T> ArrayList<ArrayList<T>> chopped(ArrayList<T> list, int numberOfSublists) {
        final int N = list.size();
        // to iterate by minimum 1
        int L = Math.max(N/numberOfSublists, 1);
        if (N%numberOfSublists!=0){
            if (N>numberOfSublists) L++;
            logger.warn("[SERVER][AsyncThreadedMsgSender] - number of WorkerManagerMetas sub-lists is not divisible by " + numberOfSublists);
            System.err.println("[SERVER][AsyncThreadedMsgSender] - number of WorkerManagerMetas sub-lists is not divisible by " + numberOfSublists);
        }
        ArrayList<ArrayList<T>> parts = new ArrayList<ArrayList<T>>();

        for (int i = 0; i < N; i += L) {
            final int iNext = i + L;
            parts.add(new ArrayList<T>(list.subList(i, Math.min(N, iNext))));
        }

        for (ArrayList<T> part : parts) {
            if (parts.get(0).size() != part.size()) {
                logger.warn("[SERVER][AsyncThreadedMsgSender] - WorkerManagerMetas list is not equally divided between threads.");
                System.err.println("[SERVER][AsyncThreadedMsgSender] - WorkerManagerMetas list is not equally divided between threads.");
            }
        }
        return parts;
    }


    ArrayList<WorkerManagerMeta> workerManagerMetas;
    ArrayList<ArrayList<WorkerManagerMeta>> dividedWorkerManagerMetas;
    private int numberOfWorkerManagers;
    private int numberOfSendingUnits;
    HashMap<String, ArrayList<WorkerMeta>> workerMetasInWorkerManager;
    ThreadPoolExecutor executor;
    private static Logger logger;


    // Used only by server to sync sending messages to all workers in workerManagerMetas
    public AsyncThreadedMsgSenderToWorkerManager(ArrayList<WorkerManagerMeta> workerManagerMetas){

        logger = LogManager.getLogger("ServerLogger");
        this.workerManagerMetas = workerManagerMetas;
        this.numberOfWorkerManagers = workerManagerMetas.size();
        this.numberOfSendingUnits = Math.min(numberOfWorkerManagers, 24);
        this.dividedWorkerManagerMetas = chopped(workerManagerMetas, numberOfSendingUnits);
        if (dividedWorkerManagerMetas.size() != numberOfSendingUnits) {
            logger.error("[SERVER][AsyncThreadedMsgSender] - dividedWorkerManagerMetas.size() is not equal to numberOfSendingUnits.");
            System.err.println("[SERVER][AsyncThreadedMsgSender] - dividedWorkerManagerMetas.size() is not equal to numberOfSendingUnits.");
            numberOfSendingUnits = dividedWorkerManagerMetas.size();
        }
        this.executor =  new ThreadPoolExecutor(numberOfSendingUnits, numberOfSendingUnits,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        logger.info("AsyncThreadedMsgSender() initialized: numberOfWorkerManagers=" + numberOfWorkerManagers
                + ", numberOfSendingUnits=" + numberOfSendingUnits);
    }

    public void sendOutToAllWorkerManagers(Message_SWM_Setup template, SendMessageType msgType, int messageHash) {
        if (executor.getActiveCount() != 0) {
            logger.warn("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
            System.err.println("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
        }

        Thread t = Thread.currentThread();
        logger.info("Thread: " + t.getName() + ", hash of SW_Setup message: " + messageHash);


        if (workerManagerMetas.size() > 0) {
            logger.info("[SERVER][AsyncThreadedMsgSender] - msgType: " + msgType.toString()
                    + ", send by: " + numberOfSendingUnits
                    + ", task Count: " + executor.getTaskCount()
                    + ", completedTaskCount: " + executor.getCompletedTaskCount());

            for (int i = 0; i < numberOfSendingUnits; ++i) {
                executor.execute(
                        new SendMessagesManagersUnit(
                                template,
                                i,
                                dividedWorkerManagerMetas.get(i),
                                msgType,
                                messageHash)
                );
            }

        }
    }

    public void sendOutWorkersSetupToAllWorkerManagers(Message_SWM_WorkerSetup template, SendMessageType msgType, int messageHash, HashMap<String, ArrayList<WorkerMeta>> workerMetasInWorkerManager) {
        if (executor.getActiveCount() != 0) {
            logger.warn("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
            System.err.println("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
        }

        Thread t = Thread.currentThread();
        logger.info("Thread: " + t.getName() + ", hash of SW_Setup message: " + messageHash);

        this.workerMetasInWorkerManager = workerMetasInWorkerManager;
        if (workerManagerMetas.size() > 0) {
            logger.info("[SERVER][AsyncThreadedMsgSender] - msgType: " + msgType.toString()
                    + ", send by: " + numberOfSendingUnits
                    + ", task Count: " + executor.getTaskCount()
                    + ", completedTaskCount: " + executor.getCompletedTaskCount());
            for (int i = 0; i < numberOfSendingUnits; ++i) {
                executor.execute(
                        new SendMessagesManagersUnit(
                                template,
                                i,
                                dividedWorkerManagerMetas.get(i),
                                msgType,
                                messageHash,
                                workerMetasInWorkerManager)
                );
            }

        }
    }

    public void sendOutToAllWorkersManagers(SendMessageType msgType, int step){
        if (executor.getActiveCount() != 0) {
            logger.warn("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
            System.err.println("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out " + msgType.toString());
        }

        if (workerManagerMetas.size() > 0) {
            logger.info("[SERVER][AsyncThreadedMsgSender] - msgType: " + msgType.toString()
                    + ", send by: " + numberOfSendingUnits
                    + ", task Count: " + executor.getTaskCount()
                    + ", completedTaskCount: " + executor.getCompletedTaskCount());

            for (int i = 0; i < numberOfSendingUnits; ++i) {
                executor.execute(
                        new SendMessagesManagersUnit(
                                i,
                                dividedWorkerManagerMetas.get(i),
                                msgType,
                                step,
                                workerMetasInWorkerManager)
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




    public void sendOutToAllCreateWorkerToWorkerManagers() {
        if (executor.getActiveCount() != 0) {
            logger.warn("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out - msgType: CreateWorkers");
            System.err.println("[SERVER][AsyncThreadedMsgSender] - Server was still sending messages " + executor.getActiveCount()
                    + " and tries to send out - msgType: CreateWorkers");
        }

        Thread t = Thread.currentThread();
        logger.info("Thread: " + t.getName());

        if (workerManagerMetas.size() > 0) {
            logger.info("[SERVER][AsyncThreadedMsgSender] - msgType: CreateWorkers"
                    + ", send by: " + numberOfSendingUnits
                    + ", task Count: " + executor.getTaskCount()
                    + ", completedTaskCount: " + executor.getCompletedTaskCount());

            for (int i = 0; i < numberOfSendingUnits; ++i) {
                executor.execute(
                        new SendMessagesManagersUnit(
                                i,
                                dividedWorkerManagerMetas.get(i))
                );
            }

        }
    }
}
