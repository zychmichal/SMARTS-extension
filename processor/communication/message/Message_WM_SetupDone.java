package processor.communication.message;

public class Message_WM_SetupDone {
    public String workerManagerName;


    public Message_WM_SetupDone() {
    }

    public Message_WM_SetupDone(final String name) {
        workerManagerName = name;
    }
}