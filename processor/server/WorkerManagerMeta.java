package processor.server;

import processor.communication.MessageSender;


public class WorkerManagerMeta {
    public String name;
    public MessageSender sender;
    public WorkerState state = WorkerState.NEW;


    public WorkerManagerMeta(final String name, final String address, final int port, String ownerName) {
        this.name = name;
        sender = new MessageSender(address, port, ownerName);
    }

    public void send(final Object message) {
        sender.send(message);
    }

    public synchronized void setState(final WorkerState state) {
        this.state = state;
    }


}
