package processor.communication.message;

import common.Settings;
import processor.server.WorkerMeta;

import java.util.ArrayList;

public class Message_SWM_WorkerSetup {
    public String workerManagerName;
    public ArrayList<Message_SW_Setup> messages;
    public ArrayList<SerializableWorkerMetadata> metadataWorkers = new ArrayList<>();
    public int startStep;
    public boolean isNewEnvironment;
    public String partitionType;


    public Message_SWM_WorkerSetup(final ArrayList<WorkerMeta> workers, final int step, String name, ArrayList<Message_SW_Setup> messages){
        this.messages = messages;
        workerManagerName = name;
        this.metadataWorkers = appendMetadataOfWorkers(workers);
        this.startStep = step;
        isNewEnvironment = Settings.isNewEnvironment;
        partitionType = Settings.partitionType;
    }

    public Message_SWM_WorkerSetup(final ArrayList<WorkerMeta> workers, String name, ArrayList<Message_SW_Setup> messages){
        this.messages = messages;
        workerManagerName = name;
        metadataWorkers = appendMetadataOfWorkers(workers);
    }

    public Message_SWM_WorkerSetup(){}

    public Message_SWM_WorkerSetup(Message_SWM_WorkerSetup template, ArrayList<WorkerMeta> workerMetas){
        this.workerManagerName = workerMetas.get(0).workerManagerName;
        this.metadataWorkers = template.metadataWorkers;
        this.startStep = template.startStep;
        isNewEnvironment = template.isNewEnvironment;
        partitionType = template.partitionType;
        ArrayList<Message_SW_Setup> tmp = new ArrayList<>();
        if(template.messages.size() != workerMetas.size()) System.out.println("ERROR NEW WM HAVE DIFFERENT WORKERS SIZE!");
        for(int i=0; i<workerMetas.size(); i++){
            tmp.add(new Message_SW_Setup(template.messages.get(i), workerMetas.get(i), false));
        }
        this.messages = tmp;
    }

    ArrayList<SerializableWorkerMetadata> appendMetadataOfWorkers(final ArrayList<WorkerMeta> workers) {
        final ArrayList<SerializableWorkerMetadata> listSerializableWorkerMetadata = new ArrayList<>();
        for (final WorkerMeta worker : workers) {
            listSerializableWorkerMetadata.add(new SerializableWorkerMetadata(worker));
        }
        return listSerializableWorkerMetadata;
    }

    public int generateHash(){
        int hashCode = 1;
        for (SerializableWorkerMetadata e : this.metadataWorkers) {
			hashCode = 31 * hashCode + (e == null ? 0 : e.address.hashCode()*e.name.hashCode()*e.port);
			for (SerializableGridCell gridCell : e.gridCells) {
				hashCode = 31 * hashCode + (gridCell == null ? 0 : gridCell.row*gridCell.column);
			}
		}
        hashCode = 31 * hashCode + Boolean.hashCode(this.isNewEnvironment);
		hashCode = 31 * hashCode + this.startStep;
		hashCode = 31 * hashCode + this.partitionType.hashCode();
       return hashCode + messages.stream().map(Message_SW_Setup::generateHash).reduce(0, Integer::sum);
    }

}
