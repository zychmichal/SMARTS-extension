package processor.communication.message;

import processor.worker.WorkerManager;

import java.util.ArrayList;

public class Message_WM_FinishCreateWorkers {
    public String workerManagerName;
    public ArrayList<SerializableWorkerInitData> workers;

    public Message_WM_FinishCreateWorkers(WorkerManager wm, ArrayList<SerializableWorkerInitData> serializableWorkers){
        workerManagerName = wm.name;
        workers = serializableWorkers;
    }

    public Message_WM_FinishCreateWorkers(){}
}
