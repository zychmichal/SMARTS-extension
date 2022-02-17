package processor.communication;

import common.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.communication.message.Message_SW_KillWorker;
import processor.communication.message.Message_SW_Serverless_Start;
import processor.communication.message.Message_SW_Serverless_Stop;
import processor.communication.message.Message_SW_Setup;
import processor.server.WorkerMeta;
import processor.server.WorkerState;
import processor.util.ServerSendersTimings;

import java.util.ArrayList;

// IMPORTANT for server use only and only in SERVERLESS MODE !!!
class SendMessagesUnit implements Runnable {

    private final ArrayList<WorkerMeta> workerMetasPart;
    private final SendMessageType msgType;
    private final int index;
    private final int step;
    private final Logger logger;
    private Message_SW_Setup template;
    private ServerSendersTimings serverSendersTimings;
    private int messageHash;

    public SendMessagesUnit(final int index, final ArrayList<WorkerMeta> workerMetasPart, SendMessageType msgType,
                            int step,
                            boolean isSharedJVM) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerMetasPart = workerMetasPart;
        this.msgType = msgType;

        // for SW_Setup message
        this.step=step;         // it is for SW_START, SW_STOP

    }

    public SendMessagesUnit(final int index, final ArrayList<WorkerMeta> workerMetasPart, SendMessageType msgType, int step) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerMetasPart = workerMetasPart;
        this.msgType = msgType;

        // for SW_Setup message
        this.step=step;         // it is for SW_START, SW_STOP

    }

    public SendMessagesUnit(Message_SW_Setup template, final int index, final ArrayList<WorkerMeta> workerMetasPart, SendMessageType msgType,
                             int messageHash) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerMetasPart = workerMetasPart;
        this.msgType = msgType;

        this.messageHash = messageHash;

        // for SW_Setup template seems to be more thread safe operation - but better to reimplement this
        this.template = template;
        //unused...
        this.step = template.startStep;
    }


    @Override
    public void run() {
        Thread.currentThread().setName("[AsyncMsgSender][T_"+index+"]");
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        switch (msgType) {
            case Message_SW_Setup -> {
                logger.info("[SERVER] - [SendMessagesUnit] - msgType: " + msgType.toString()
                        + ", to #workers: " + workerMetasPart.size());

                serverSendersTimings.startSendingSetupConfirmation();
                for (final WorkerMeta worker : workerMetasPart) {
//                    final Message_SW_Setup sw_setup = new Message_SW_Setup(workerMetasPart, worker, step,
//                            nodesToAddLight, nodesToRemoveLight, trafficHashVal);
                    final Message_SW_Setup sw_setup = new Message_SW_Setup(template, worker, true);
                    int thisMessageHash = sw_setup.generateHash();
                    if (this.messageHash != thisMessageHash) {
                        System.err.println("Sending Setup message hashes are not equal " + this.messageHash + " Unit: " + thisMessageHash);
                        logger.error("Sending Setup message hashes are not equal " + this.messageHash + " Unit: " + thisMessageHash);
                    }
                    worker.send(sw_setup);
                    logger.debug("[AsyncMsgSender]SETUP_FROM_SERVER" + sw_setup.toString());
                }
                serverSendersTimings.stopSendingSetupConfirmation(System.out);
                serverSendersTimings.logSendingSetupTimer(logger);
            }
            case Message_SW_Serverless_Start -> {
                serverSendersTimings.startSendingStart();
                final Message_SW_Serverless_Start sw_start_msg = new Message_SW_Serverless_Start(step);
                for (final WorkerMeta worker : workerMetasPart) {
                    worker.send(sw_start_msg);
                    worker.setState(WorkerState.SERVERLESS_WORKING);
                }
                serverSendersTimings.stopSendingStart(System.out);
                serverSendersTimings.logSendingStartMsgTimer(logger);
                logger.info("[SERVER] - [SendMessagesUnit] - msgType: " + msgType.toString()
                        + ", to #workers: " + workerMetasPart.size());
            }
            case Message_SW_Serverless_Stop -> {
//                serverSendersTimings.startSendingStop();
                final Message_SW_Serverless_Stop message = new Message_SW_Serverless_Stop(step);
                for (final WorkerMeta worker : workerMetasPart) {
                    worker.send(message);
                    worker.setState(WorkerState.NEW);
                }
//                serverSendersTimings.stopSendingStop(System.out);
//                serverSendersTimings.logSendingStopTimer(logger);
//                logger.info("[SERVER] - [SendMessagesUnit] - msgType: " + msgType.toString()
//                        + ", to #workers: " + workerMetasPart.size());
            }
            case Message_SW_KillWorker -> {
//                serverSendersTimings.startSendingKill();
                final Message_SW_KillWorker msg_kill = new Message_SW_KillWorker(Settings.isSharedJVM);
                for (final WorkerMeta worker : workerMetasPart) {
                    worker.send(msg_kill);
                }
//                serverSendersTimings.stopSendingKill(System.out);
//                serverSendersTimings.logSendingKillTimer(logger);
//                logger.info("[SERVER] - [SendMessagesUnit] - msgType: " + msgType.toString()
//                        + ", to #workers: " + workerMetasPart.size());
            }
            default -> {
                System.err.println("[SERVER] - AsyncThreadedMsgSender.sendToAll() - message is not defined: " + msgType
                        + ". Thread: " + Thread.currentThread().getName());
                logger.error("[SERVER] - AsyncThreadedMsgSender.sendToAll() - message is not defined: " + msgType
                        + ". Thread: " + Thread.currentThread().getName());
            }
        }
    }
}
