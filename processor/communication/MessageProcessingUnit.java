package processor.communication;

import common.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.communication.message.MessageUtil;

import java.util.concurrent.BlockingQueue;

class MessageProcessingUnit implements Runnable {
    private final BlockingQueue<String> queue;
    private final MessageHandler messageHandler;
    private final String ownerName;
    private Logger logger = null;
//        private AtomicBoolean running = new AtomicBoolean(true);
    private final ThreadGroup threadGroup;

    public MessageProcessingUnit(MessageHandler messageHandler, BlockingQueue<String> queue, String ownerName, ThreadGroup threadGroup) {
        this.messageHandler = messageHandler;
        this.queue = queue;
        this.ownerName = ownerName;
        this.logger = ownerName.toLowerCase().equals("server") ? LogManager.getLogger("ServerLogger") : LogManager.getLogger("WorkerLogger");
        this.threadGroup = threadGroup;
    }

    public void run() {
        try {
            while (true){              //while(running.get()){
                logger.debug("UNIT - Waiting for msg from msg queue... Q: " +queue.size() + ", T_g: "+ threadGroup.activeCount());
                String msg = queue.take();
                logger.debug("UNIT - GOT IT. Read msg. Q: " + queue.size()+ ", T_g: "+ threadGroup.activeCount());
                //if(ownerName.equalsIgnoreCase("server")) System.out.println("QUEUE SIZE: " + queue.size());
                //System.out.println(msg);
//                System.out.println(Thread.currentThread().getName() + ": " + msg);
                if (msg.equals(Settings.close_pill)) {
                    break;
                }
                final Object received = MessageUtil.read(msg);
                messageHandler.processReceivedMsg(received);
            }
        } catch (InterruptedException e) {
            System.err.println("["+ownerName + "][MessageProcessingUnit][PARSING-THREAD]  --  interrupted");
            e.printStackTrace();
            logger.error("EXCEPTION --- InterruptedException in IncommingConnectionBuilder: ", e);
        } catch (Exception e) {
            System.err.println("["+ownerName + "][MessageProcessingUnit][PARSING-THREAD]  --  UknownException");
            e.printStackTrace();
            logger.error("EXCEPTION --- Unknown Exception in IncommingConnectionBuilder: ", e);
        }

        logger.debug("MessageProcessingUnit closed.");
    }
}