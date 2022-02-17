package processor.communication;

import common.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.communication.message.*;
import processor.server.WorkerManagerMeta;
import processor.server.WorkerMeta;
import processor.server.WorkerState;
import processor.util.ServerSendersTimings;

import java.util.ArrayList;
import java.util.HashMap;

public class SendMessagesManagersUnit implements Runnable{

    private final ArrayList<WorkerManagerMeta> workerManagerMetasPart;
    private SendMessageType msgType;
    private boolean duringSimulation = false;
    private int step = 0;
    HashMap<String, ArrayList<WorkerMeta>> workerMetasInWorkerManager;
    private final int index;
    private final Logger logger;
    private Message_SWM_Setup template;
    private Message_SWM_WorkerSetup workerSetupTemplate;
    private boolean wmSetup = true;
    private ServerSendersTimings serverSendersTimings;
    private int messageHash;
    private boolean createWorkers = false;

    /*public SendMessagesManagersUnit(final int index, final ArrayList<WorkerManagerMeta> workerMetasPart, SendMessageType msgType,
                            int step,
                            boolean isSharedJVM) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerManagerMetasPart = workerMetasPart;
        this.msgType = msgType;

        // for SW_Setup message
        this.step=step;         // it is for SW_START, SW_STOP

    }*/

    public SendMessagesManagersUnit(final int index, final ArrayList<WorkerManagerMeta> workerMetasPart) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerManagerMetasPart = workerMetasPart;



        this.createWorkers = true;

    }

    public SendMessagesManagersUnit(final int index, final ArrayList<WorkerManagerMeta> workerManagerMetasPart, SendMessageType msgType, int step,
                                    HashMap<String, ArrayList<WorkerMeta>> workerMetasInWorkerManage) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerManagerMetasPart = workerManagerMetasPart;
        this.workerMetasInWorkerManager = workerMetasInWorkerManage;
        this.msgType = msgType;

        // for SW_Setup message
        this.wmSetup = false;
        this.duringSimulation = true;
        this.step=step;         // it is for SW_START, SW_STOP

    }

    public SendMessagesManagersUnit(Message_SWM_Setup template, final int index, final ArrayList<WorkerManagerMeta> workerMetasPart, SendMessageType msgType,
                                    int messageHash) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerManagerMetasPart = workerMetasPart;

        this.messageHash = messageHash;

        // for SW_Setup template seems to be more thread safe operation - but better to reimplement this
        this.template = template;
        //unused...
    }

    public SendMessagesManagersUnit(Message_SWM_WorkerSetup template, final int index, final ArrayList<WorkerManagerMeta> workerMetasPart, SendMessageType msgType,
                                    int messageHash, HashMap<String, ArrayList<WorkerMeta>> workerMetasInWorkerManage) {
        logger = LogManager.getLogger("ServerLogger");
        serverSendersTimings = new ServerSendersTimings(index);

        // for all
        this.index = index;
        this.workerManagerMetasPart = workerMetasPart;

        this.messageHash = messageHash;

        // for SW_Setup template seems to be more thread safe operation - but better to reimplement this
        this.workerSetupTemplate = template;
        this.wmSetup = false;
        this.workerMetasInWorkerManager = workerMetasInWorkerManage;
        //unused...
    }


    @Override
    public void run() {
        Thread.currentThread().setName("[AsyncMsgSender][T_"+index+"]");
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        logger.info("[SERVER] - [SendMessagesManagersUnit] - msgType: SWM_SETUP"
                + ", to #workerManagers: " + workerManagerMetasPart.size());
        serverSendersTimings.startSendingSetupManagerConfirmation();

        if(wmSetup){
            if (createWorkers){
                for (final WorkerManagerMeta workerManager : workerManagerMetasPart){
                    workerManager.send(new Message_SWM_CreateWorkers());
                    logger.debug("[AsyncMsgSender] CREATEWORKER_FROM_SERVER");
                }
            }else{
                for (final WorkerManagerMeta workerManager : workerManagerMetasPart){
                    final Message_SWM_Setup swm_setup = new Message_SWM_Setup(template);
                    int thisMessageHash = swm_setup.generateHash();
                    if (this.messageHash != thisMessageHash) {
                        System.err.println("Sending Setup message SWM SETUP hashes are not equal " + this.messageHash + " Unit: " + thisMessageHash);
                        logger.error("Sending Setup message hashes are not equal " + this.messageHash + " Unit: " + thisMessageHash);
                    }
                    workerManager.send(swm_setup);
                    logger.debug("[AsyncMsgSender]SETUP_FROM_SERVER" + swm_setup.toString());
                }
            }
        } else {
            if (duringSimulation){
                switch(msgType){
                    case Message_SW_Serverless_Start -> {
                        serverSendersTimings.startSendingStart();
                        final Message_SW_Serverless_Start sw_start_msg = new Message_SW_Serverless_Start(step);
                        for (final WorkerManagerMeta workerManager : workerManagerMetasPart) {
                            workerManager.send(sw_start_msg);
                            workerMetasInWorkerManager.get(workerManager.name).forEach( worker -> {
                                worker.setState(WorkerState.SERVERLESS_WORKING);
                            });
                        }
                        serverSendersTimings.stopSendingStart(System.out);
                        serverSendersTimings.logSendingStartMsgTimer(logger);
                        logger.info("[SERVER] - [SendMessagesUnit] - msgType: " + msgType.toString()
                                + ", to #workerManagers: " + workerManagerMetasPart.size());
                    }
                    case Message_SW_Serverless_Stop -> {
//                serverSendersTimings.startSendingStop();
                        final Message_SW_Serverless_Stop message = new Message_SW_Serverless_Stop(step);
                        for (final WorkerManagerMeta workerManager : workerManagerMetasPart) {
                            workerManager.send(message);
                            workerMetasInWorkerManager.get(workerManager.name).forEach( worker -> worker.setState(WorkerState.SERVERLESS_WORKING));
                        }
//                serverSendersTimings.stopSendingStop(System.out);
//                serverSendersTimings.logSendingStopTimer(logger);
//                logger.info("[SERVER] - [SendMessagesUnit] - msgType: " + msgType.toString()
//                        + ", to #workers: " + workerMetasPart.size());
                    }
                    case Message_SW_KillWorker -> {
//                serverSendersTimings.startSendingKill();
                        final Message_SW_KillWorker msg_kill = new Message_SW_KillWorker(Settings.isSharedJVM);
                        for (final WorkerManagerMeta workerManager : workerManagerMetasPart) {
                            workerManager.send(msg_kill);
                        }
//                serverSendersTimings.stopSendingKill(System.out);
//                serverSendersTimings.logSendingKillTimer(logger);
//                logger.info("[SERVER] - [SendMessagesUnit] - msgType: " + msgType.toString()
//                        + ", to #workers: " + workerMetasPart.size());
                    }
                }

            } else {
                for (final WorkerManagerMeta workerManager : workerManagerMetasPart){
                    final Message_SWM_WorkerSetup swm_worker_setup = new Message_SWM_WorkerSetup(workerSetupTemplate, workerMetasInWorkerManager.get(workerManager.name));
                    int thisMessageHash = swm_worker_setup.generateHash();
                    if (this.messageHash != thisMessageHash) {
                        System.err.println("Sending Setup message hashes SWM WORKER SETUP are not equal " + this.messageHash + " Unit: " + thisMessageHash);
                        logger.error("Sending Setup message hashes are not equal " + this.messageHash + " Unit: " + thisMessageHash);
                    }
                    workerManager.send(swm_worker_setup);
                    logger.debug("[AsyncMsgSender] SETUP_FROM_SERVER" + swm_worker_setup);
                }
                duringSimulation=true;
            }


        }
        serverSendersTimings.stopSendingSetupConfirmation(System.out);
        serverSendersTimings.logSendingSetupTimer(logger);
    }
}
