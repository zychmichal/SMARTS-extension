package processor.communication.message;

import processor.worker.Worker;

public class SerializableWorkerInitData {
    public String name;
    public String address;
    public int port;

    public SerializableWorkerInitData(){}

    public SerializableWorkerInitData(Worker worker){
        this.name = worker.name;
        this.address = worker.address;
        this.port = worker.listeningPort;
    }
}
