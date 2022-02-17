package processor.communication;

import common.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class initializes and manages MessageProcessingUnits life-cycle with single msg blockingQueue
 *
 */
public class MessageProcessor{

    private MessageHandler messageHandler = null;
    private final String ownerName;      // handler's name
    private final int queueSize = Integer.MAX_VALUE;
//    private BlockingQueue<String> queue;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(queueSize);
    private final ThreadGroup threadGroup = new ThreadGroup("[MessageProcessors]");
//    ArrayList<MessageProcessingUnit> msgProcessingUnitList = new ArrayList<MessageProcessingUnit>(50);
    private int numberOfProccesingUnits = 24;
    private Logger logger = null;
//    boolean running = true;

    /**
     * @param ownerName
     *            Handler's name : [Server / Worker(id)]
     * @param messageHandler
     *            Entity that processes received messages (reference to specific server/worker(id)).
     */
    public MessageProcessor(final String ownerName, int numberOfProccesingUnits, final MessageHandler messageHandler) {
        this.ownerName = ownerName;
        this.numberOfProccesingUnits = numberOfProccesingUnits;
        this.messageHandler = messageHandler;
        this.logger = ownerName.toLowerCase().equals("server") ? LogManager.getLogger("ServerLogger") : LogManager.getLogger("WorkerLogger");
    }

    /**
     * Run defined number of thread to handle incoming msgs from queue
     */
    public void runAllProccessors() {
        try {
            for (int i = 0; i < numberOfProccesingUnits; ++i){
                new Thread(
                        threadGroup,
                        new MessageProcessingUnit(messageHandler, queue, ownerName, threadGroup),
                        "[" + ownerName + "]" + threadGroup.getName()+"[T_" + i + "]"
                ).start();
            }
        } catch (final Exception e) {
            System.err.println("["+ownerName + "][" + threadGroup.getName() + "]");
            e.printStackTrace();
            logger.error("EXCEPTION in MessageProcessor.runAllProccessors() owner: " + ownerName
                    + " --- Unknown Exception: ", e);
        }
    }

    public void poisonAllProcessors() {
        if (getSynQueueSize() != 0) {
            System.err.println("[" + ownerName + "][" + threadGroup.getName() + "]" + "-there are still few elements left in queue");
            logger.warn("WARNING Queue is not empty (" + getSynQueueSize() + ") and posioning has started - " + ownerName);
        }

        new Thread(() -> {
            for (int i = 0; i < numberOfProccesingUnits; ++i) {
                try {
                    queue.put(Settings.close_pill);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("MessageProcessor Exception while poisoning ProcessingThreads qeueu - " + ownerName, e);
                }
            }
        }).start();
    }

    /*
     *   It uses remainingCapacity method which synchronizes on queue (better not to use when queue is under load)
     */
    public int getSynQueueSize(){
        return queueSize - queue.remainingCapacity();
        // analogus just simply   * return queue.size()
    }

    public int getQueueSize(){
        return queue.size();
        // analogus just simply   * return queue.size()
    }

    public synchronized BlockingQueue<String> getQueue(){
        return queue;
    }

    public int getNumberOfProccesingUnits() {
        return numberOfProccesingUnits;
    }

    public String getOwnerName(){
        return ownerName;
    }

    @Override
    public String toString() {
        return "MessageProcessor{" +
                ", queue=" + queue +
                ", #queueElements=" + getQueueSize() +
                ", #numberOfProccesingUnits=" + numberOfProccesingUnits +
                '}';
    }
}
