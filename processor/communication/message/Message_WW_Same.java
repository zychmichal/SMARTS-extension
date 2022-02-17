package processor.communication.message;

public class Message_WW_Same {
    public String workerName;
    public int step;

    public Message_WW_Same(){}

    public Message_WW_Same(final String name, final int step) {
        workerName = name;
        this.step = step;
    }
}
