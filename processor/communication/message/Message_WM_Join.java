package processor.communication.message;

public class Message_WM_Join {
    public String workerManagerName;
    public String workerManagerAddress;
    public int workerManagerPort;

    public Message_WM_Join() {

    }

    public Message_WM_Join(final String name, final String address, final int port) {
        workerManagerName = name;
        workerManagerAddress = address;
        workerManagerPort = port;
    }
}
